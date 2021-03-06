%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_processor).

-export([info/2, initial_state/2, initial_state/4,
         process_frame/2, amqp_pub/2, amqp_callback/2, send_will/1,
         close_connection/1, close_connection_and_relay/1, http_relay/3]).

%% for testing purposes
-export([get_vhost_username/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt_frame.hrl").
-include("rabbit_mqtt.hrl").

-define(APP, rabbitmq_mqtt).
-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).

initial_state(Socket, SSLLoginName) ->
    initial_state(Socket, SSLLoginName,
        adapter_info(Socket, 'MQTT'),
        fun send_client/2).

initial_state(Socket, SSLLoginName,
              AdapterInfo0 = #amqp_adapter_info{additional_info = Extra},
              SendFun) ->
    %% MQTT connections use exactly one channel. The frame max is not
    %% applicable and there is no way to know what client is used.
    AdapterInfo = AdapterInfo0#amqp_adapter_info{additional_info = [
        {channels, 1},
        {channel_max, 1},
        {frame_max, 0},
        {client_properties,
         [{<<"product">>, longstr, <<"MQTT client">>}]} | Extra]},
    #proc_state{ unacked_pubs   = gb_trees:empty(),
                 awaiting_ack   = gb_trees:empty(),
                 message_id     = 1,
                 subscriptions  = dict:new(),
                 consumer_tags  = {undefined, undefined},
                 channels       = {undefined, undefined},
                 exchange       = rabbit_mqtt_util:env(exchange),
                 socket         = Socket,
                 adapter_info   = AdapterInfo,
                 ssl_login_name = SSLLoginName,
                 send_fun       = SendFun }.

info(client_id, #proc_state{ client_id = ClientId }) -> ClientId.

packet_log(#mqtt_frame{fixed = Fixed,
                       variable = Variable}, 
           #proc_state{client_id = ClientId,
                       auth_state = #auth_state{username = Username}}) ->
    rabbit_log:log(mqtt_packet, debug, "~w ~w ~w ~p ~p", [self(), Fixed, Variable, ClientId, Username]);
packet_log(#mqtt_frame{fixed = Fixed,
                       variable = Variable}, 
           _PState) -> rabbit_log:log(mqtt_packet, debug, "~w ~w ~p", [self(), Fixed, Variable]);
packet_log(Frame, _PState) -> rabbit_log:log(mqtt_packet, debug, "~w ~p", [self(), Frame]).

process_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState = #proc_state{ connection = undefined } )
  when Type =/= ?CONNECT ->
    {error, connect_expected, PState};
process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState) ->
    packet_log(Frame, PState),
    case process_request(Type, Frame, PState) of
        {ok, PState1} -> {ok, PState1, PState1#proc_state.connection};
        Ret -> Ret
    end.

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                           username   = Username,
                                           password   = Password,
                                           proto_ver  = ProtoVersion,
                                           clean_sess = CleanSess,
                                           client_id  = ClientId0,
                                           keep_alive = Keepalive} = Var},
                PState = #proc_state{ ssl_login_name = SSLLoginName,
                                      send_fun = SendFun }) ->
    ClientId = case ClientId0 of
                   []    -> rabbit_mqtt_util:gen_client_id();
                   [_|_] -> ClientId0
               end,
    {ReturnCode, PState1} =
        case {lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)),
              ClientId0 =:= [] andalso CleanSess =:= false} of
            {false, _} ->
                {?CONNACK_PROTO_VER, PState};
            {_, true} ->
                {?CONNACK_INVALID_ID, PState};
            _ ->
                case creds(Username, Password, SSLLoginName) of
                    nocreds ->
                        rabbit_log:error("MQTT login failed - no credentials~n"),
                        {?CONNACK_CREDENTIALS, PState};
                    {UserBin, PassBin} ->
                        case process_login(UserBin, PassBin, ProtoVersion, PState) of
                            {?CONNACK_ACCEPT, Conn, VHost, AState} ->
                                 RetainerPid =
                                   rabbit_mqtt_retainer_sup:child_for_vhost(VHost),
                                link(Conn),
                                {ok, Ch} = amqp_connection:open_channel(Conn),
                                link(Ch),
                                amqp_channel:enable_delivery_flow_control(Ch),
                                ok = rabbit_mqtt_collector:register(
                                  ClientId, self()),
                                Prefetch = rabbit_mqtt_util:env(prefetch),
                                #'basic.qos_ok'{} = amqp_channel:call(
                                  Ch, #'basic.qos'{prefetch_count = Prefetch}),
                                rabbit_mqtt_reader:start_keepalive(self(), Keepalive),
                                {?CONNACK_ACCEPT,
                                 maybe_clean_sess(
                                   PState #proc_state{ will_msg   = make_will_msg(Var),
                                                       clean_sess = CleanSess,
                                                       channels   = {Ch, undefined},
                                                       connection = Conn,
                                                       client_id  = ClientId,
                                                       retainer_pid = RetainerPid,
                                                       auth_state = AState})};
                            ConnAck ->
                                {ConnAck, PState}
                        end
                end
        end,
    SendFun(#mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?CONNACK},
                         variable = #mqtt_frame_connack{
                                     return_code = ReturnCode }}, PState1),
    {ok, PState1};

process_request(?PUBACK,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{ message_id = MessageId }},
                #proc_state{ channels     = {Channel, _},
                             awaiting_ack = Awaiting } = PState) ->
    %% tag can be missing because of bogus clients and QoS downgrades
    case gb_trees:is_defined(MessageId, Awaiting) of
      false ->
        {ok, PState};
      true ->
        Tag = gb_trees:get(MessageId, Awaiting),
        amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag = Tag }),
        {ok, PState #proc_state{ awaiting_ack = gb_trees:delete( MessageId, Awaiting)}}
    end;

process_request(?PUBLISH,
                Frame = #mqtt_frame{ 
                    fixed = Fixed = #mqtt_frame_fixed{ qos = ?QOS_2 }}, 
                PState) ->
    % Downgrade QOS_2 to QOS_1
    process_request(?PUBLISH, 
                    Frame#mqtt_frame{
                        fixed = Fixed#mqtt_frame_fixed{ qos = ?QOS_1 }},
                    PState);
process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos    = Qos,
                                             retain = Retain,
                                             dup    = Dup },
                  variable = #mqtt_frame_publish{ topic_name = Topic,
                                                  message_id = MessageId },
                  payload = Payload },
                  PState = #proc_state{retainer_pid = RPid}) ->
    check_publish_or_die(Topic, fun() ->
        Msg = #mqtt_msg{retain     = Retain,
                        qos        = Qos,
                        topic      = Topic,
                        dup        = Dup,
                        message_id = MessageId,
                        payload    = Payload},
        Result = amqp_pub(Msg, PState),
        case Retain of
          false -> ok;
          true  -> hand_off_to_retainer(RPid, Topic, Msg)
        end,
        {ok, Result}
    end, PState);

process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{
                              message_id  = MessageId,
                              topic_table = Topics},
                  payload = undefined},
                #proc_state{channels = {Channel, _},
                            exchange = Exchange,
                            retainer_pid = RPid,
                            message_id = ProcMessageId,
                            send_fun = SendFun } = PState0) ->
    check_subscribe_or_die(Topics, fun() ->
        {QosResponse, PState1} =
            lists:foldl(fun (#mqtt_topic{name = TopicName,
                                         qos  = Qos}, {QosList, PState}) ->
                           SupportedQos = supported_subs_qos(Qos),
                           {Queue, #proc_state{subscriptions = Subs} = PState1} =
                               ensure_queue(SupportedQos, PState),
                           Binding = #'queue.bind'{
                                       queue       = Queue,
                                       exchange    = Exchange,
                                       routing_key = rabbit_mqtt_util:mqtt2amqp(
                                                       TopicName)},
                           #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
                           {[SupportedQos | QosList],
                            PState1 #proc_state{subscriptions =
                                                dict:append(TopicName, SupportedQos, Subs)}}
                       end, {[], PState0}, Topics),
        SendFun(#mqtt_frame{fixed    = #mqtt_frame_fixed{type = ?SUBACK},
                            variable = #mqtt_frame_suback{
                                        message_id = MessageId,
                                        qos_table  = QosResponse}}, PState1),
        %% we may need to send up to length(Topics) messages.
        %% if QoS is > 0 then we need to generate a message id,
        %% and increment the counter.
        N = lists:foldl(fun (Topic, Acc) ->
                          case maybe_send_retained_message(RPid, Topic, Acc, PState1) of
                            {true, X} -> Acc + X;
                            false     -> Acc
                          end
                        end, ProcMessageId, Topics),
        {ok, PState1#proc_state{message_id = N}}
    end, PState0);

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined }, #proc_state{ channels      = {Channel, _},
                                                      exchange      = Exchange,
                                                      client_id     = ClientId,
                                                      subscriptions = Subs0,
                                                      send_fun      = SendFun } = PState) ->
    Queues = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Subs1 =
    lists:foldl(
      fun (#mqtt_topic{ name = TopicName }, Subs) ->
        QosSubs = case dict:find(TopicName, Subs) of
                      {ok, Val} when is_list(Val) -> lists:usort(Val);
                      error                       -> []
                  end,
        lists:foreach(
          fun (QosSub) ->
                  Queue = element(QosSub + 1, Queues),
                  Binding = #'queue.unbind'{
                              queue       = Queue,
                              exchange    = Exchange,
                              routing_key =
                                  rabbit_mqtt_util:mqtt2amqp(TopicName)},
                  #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding)
          end, QosSubs),
        dict:erase(TopicName, Subs)
      end, Subs0, Topics),
    SendFun(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type       = ?UNSUBACK },
                         variable = #mqtt_frame_suback{ message_id = MessageId }},
                PState),
    {ok, PState #proc_state{ subscriptions = Subs1 }};

process_request(?PINGREQ, #mqtt_frame{ variable = <<Status>> }, 
                PState = #proc_state{ auth_state = #auth_state{ username = Username }, 
                                      client_id = ClientId,
                                      client_status = PrevClientStatus,
                                      send_fun = SendFun}) ->
    case PrevClientStatus of
        Status -> void;
        _ -> relay_client_status(ClientId, Username, Status)
    end,
    SendFun(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }},
                PState),
    {ok, PState #proc_state{client_status = Status}};

process_request(?PINGREQ, #mqtt_frame{}, #proc_state{ send_fun = SendFun } = PState) ->
    SendFun(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }},
                PState),
    {ok, PState};

process_request(?DISCONNECT, #mqtt_frame{}, PState) ->
    {stop, PState}.

%%----------------------------------------------------------------------------

hand_off_to_retainer(RetainerPid, Topic, #mqtt_msg{payload = <<"">>}) ->
  rabbit_mqtt_retainer:clear(RetainerPid, Topic),
  ok;
hand_off_to_retainer(RetainerPid, Topic, Msg) ->
  rabbit_mqtt_retainer:retain(RetainerPid, Topic, Msg),
  ok.

maybe_send_retained_message(RPid, #mqtt_topic{name = S, qos = SubscribeQos}, MsgId,
                            #proc_state{ send_fun = SendFun } = PState) ->
  case rabbit_mqtt_retainer:fetch(RPid, S) of
    undefined -> false;
    Msg       ->
                %% calculate effective QoS as the lower value of SUBSCRIBE frame QoS
                %% and retained message QoS. The spec isn't super clear on this, we
                %% do what Mosquitto does, per user feedback.
                Qos = erlang:min(SubscribeQos, Msg#mqtt_msg.qos),
                Id = case Qos of
                  ?QOS_0 -> undefined;
                  ?QOS_1 -> MsgId
                end,
                SendFun(#mqtt_frame{fixed = #mqtt_frame_fixed{
                    type = ?PUBLISH,
                    qos  = Qos,
                    dup  = false,
                    retain = Msg#mqtt_msg.retain
                 }, variable = #mqtt_frame_publish{
                    message_id = Id,
                    topic_name = S
                 },
                 payload = Msg#mqtt_msg.payload}, PState),
                 case Qos of
                   ?QOS_0 -> false;
                   ?QOS_1 -> {true, 1}
                 end
  end.

amqp_callback({#'basic.deliver'{ consumer_tag = ConsumerTag,
                                 delivery_tag = DeliveryTag,
                                 routing_key  = RoutingKey },
               #amqp_msg{ props = #'P_basic'{ headers = Headers },
                          payload = Payload },
               DeliveryCtx} = Delivery,
              #proc_state{ channels      = {Channel, _},
                           awaiting_ack  = Awaiting,
                           message_id    = MsgId,
                           send_fun      = SendFun } = PState) ->
    amqp_channel:notify_received(DeliveryCtx),
    case {delivery_dup(Delivery), delivery_qos(ConsumerTag, Headers, PState)} of
        {true, {?QOS_0, ?QOS_1}} ->
            amqp_channel:cast(
              Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
            {ok, PState};
        {true, {?QOS_0, ?QOS_0}} ->
            {ok, PState};
        {Dup, {DeliveryQos, _SubQos} = Qos}     ->
            SendFun(
              #mqtt_frame{ fixed = #mqtt_frame_fixed{
                                     type = ?PUBLISH,
                                     qos  = DeliveryQos,
                                     dup  = Dup },
                           variable = #mqtt_frame_publish{
                                        message_id =
                                          case DeliveryQos of
                                              ?QOS_0 -> undefined;
                                              ?QOS_1 -> MsgId
                                          end,
                                        topic_name =
                                          rabbit_mqtt_util:amqp2mqtt(
                                            RoutingKey) },
                           payload = Payload}, PState),
              case Qos of
                  {?QOS_0, ?QOS_0} ->
                      {ok, PState};
                  {?QOS_1, ?QOS_1} ->
                      {ok,
                       next_msg_id(
                         PState #proc_state{
                           awaiting_ack =
                             gb_trees:insert(MsgId, DeliveryTag, Awaiting)})};
                  {?QOS_0, ?QOS_1} ->
                      amqp_channel:cast(
                        Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
                      {ok, PState}
              end
    end;

amqp_callback(#'basic.ack'{ multiple = true, delivery_tag = Tag } = Ack,
              PState = #proc_state{ unacked_pubs = UnackedPubs,
                                    send_fun     = SendFun }) ->
    case gb_trees:size(UnackedPubs) > 0 andalso
         gb_trees:take_smallest(UnackedPubs) of
        {TagSmall, MsgId, UnackedPubs1} when TagSmall =< Tag ->
            SendFun(
              #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                           variable = #mqtt_frame_publish{ message_id = MsgId }},
              PState),
            amqp_callback(Ack, PState #proc_state{ unacked_pubs = UnackedPubs1 });
        _ ->
            {ok, PState}
    end;

amqp_callback(#'basic.ack'{ multiple = false, delivery_tag = Tag },
              PState = #proc_state{ unacked_pubs = UnackedPubs,
                                    send_fun     = SendFun }) ->
    SendFun(
      #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                   variable = #mqtt_frame_publish{
                                message_id = gb_trees:get(
                                               Tag, UnackedPubs) }}, PState),
    {ok, PState #proc_state{ unacked_pubs = gb_trees:delete(Tag, UnackedPubs) }}.

delivery_dup({#'basic.deliver'{ redelivered = Redelivered },
              #amqp_msg{ props = #'P_basic'{ headers = Headers }},
              _DeliveryCtx}) ->
    case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-dup">>) of
        undefined   -> Redelivered;
        {bool, Dup} -> Redelivered orelse Dup
    end.

next_msg_id(PState = #proc_state{ message_id = 16#ffff }) ->
    PState #proc_state{ message_id = 1 };
next_msg_id(PState = #proc_state{ message_id = MsgId }) ->
    PState #proc_state{ message_id = MsgId + 1 }.

%% decide at which qos level to deliver based on subscription
%% and the message publish qos level. non-MQTT publishes are
%% assumed to be qos 1, regardless of delivery_mode.
delivery_qos(Tag, _Headers,  #proc_state{ consumer_tags = {Tag, _} }) ->
    {?QOS_0, ?QOS_0};
delivery_qos(Tag, Headers,   #proc_state{ consumer_tags = {_, Tag} }) ->
    case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-publish-qos">>) of
        {byte, Qos} -> {lists:min([Qos, ?QOS_1]), ?QOS_1};
        undefined   -> {?QOS_1, ?QOS_1}
    end.

maybe_clean_sess(PState = #proc_state { clean_sess = false }) ->
    {_Queue0, PState0} = ensure_queue(?QOS_0, PState),
    {_Queue1, PState1} = ensure_queue(?QOS_1, PState0),
    PState1;
maybe_clean_sess(PState = #proc_state { clean_sess = true,
                                        connection = Conn,
                                        client_id  = ClientId }) ->
    {Queue0, Queue1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    catch amqp_channel:call(Channel, #'queue.delete'{ queue = Queue0 }),
    catch amqp_channel:call(Channel, #'queue.delete'{ queue = Queue1 }),
    amqp_channel:close(Channel),
    PState.

%%----------------------------------------------------------------------------

make_will_msg(#mqtt_frame_connect{ will_flag   = false }) ->
    undefined;
make_will_msg(#mqtt_frame_connect{ will_retain = Retain,
                                   will_qos    = Qos,
                                   will_topic  = Topic,
                                   will_msg    = Msg }) ->
    #mqtt_msg{ retain  = Retain,
               qos     = Qos,
               topic   = Topic,
               dup     = false,
               payload = Msg }.

process_login(UserBin, PassBin, ProtoVersion,
              #proc_state{ channels     = {undefined, undefined},
                           socket       = Sock,
                           adapter_info = AdapterInfo }) ->
    {VHost, UsernameBin} = get_vhost_username(UserBin),
    case amqp_connection:start(#amqp_params_direct{
                                  username     = UsernameBin,
                                  password     = PassBin,
                                  virtual_host = VHost,
                                  adapter_info = set_proto_version(AdapterInfo, ProtoVersion)}) of
        {ok, Connection} ->
            case rabbit_access_control:check_user_loopback(UsernameBin, Sock) of
                ok          ->
                  {ok, User} = rabbit_access_control:check_user_login(
                                 UsernameBin,
                                 case PassBin of
                                   none -> [];
                                   P -> [{password,P}]
                                 end),
                  {?CONNACK_ACCEPT, Connection, VHost, #auth_state{
                                                         user = User,
                                                         username = UsernameBin,
                                                         vhost = VHost}};
                not_allowed -> amqp_connection:close(Connection),
                               rabbit_log:warning(
                                 "MQTT login failed for ~p access_refused "
                                 "(access must be from localhost)~n",
                                 [binary_to_list(UsernameBin)]),
                               ?CONNACK_AUTH
            end;
        {error, {auth_failure, Explanation}} ->
            rabbit_log:error("MQTT login failed for ~p auth_failure: ~s~n",
                             [binary_to_list(UserBin), Explanation]),
            ?CONNACK_CREDENTIALS;
        {error, access_refused} ->
            rabbit_log:warning("MQTT login failed for ~p access_refused "
                               "(vhost access not allowed)~n",
                               [binary_to_list(UserBin)]),
            ?CONNACK_AUTH
    end.

get_vhost_username(UserBin) ->
    Default = {rabbit_mqtt_util:env(vhost), UserBin},
    case application:get_env(?APP, ignore_colons_in_username) of
        {ok, true} -> Default;
        _ ->
            %% split at the last colon, disallowing colons in username
            case re:split(UserBin, ":(?!.*?:)") of
                [Vhost, UserName] -> {Vhost,  UserName};
                [UserBin]         -> Default
            end
    end.

creds(User, Pass, SSLLoginName) ->
    DefaultUser   = rabbit_mqtt_util:env(default_user),
    DefaultPass   = rabbit_mqtt_util:env(default_pass),
    {ok, Anon}    = application:get_env(?APP, allow_anonymous),
    {ok, TLSAuth} = application:get_env(?APP, ssl_cert_login),
    U = case {User =/= undefined,
              is_binary(DefaultUser),
              Anon =:= true,
              (TLSAuth andalso SSLLoginName =/= none)} of
             %% username provided
             {true,  _,    _,    _}     -> list_to_binary(User);
             %% anonymous, default user is configured, no TLS
             {false, true, true, false} -> DefaultUser;
             %% no username provided, TLS certificate is present,
             %% rabbitmq_mqtt.ssl_cert_login is true
             {false, _,    _,    true}  -> SSLLoginName;
             _                          -> nocreds
        end,
    case U of
        nocreds ->
            nocreds;
        _ ->
            case {Pass =/= undefined,
                  is_binary(DefaultPass),
                  Anon =:= true,
                  TLSAuth} of
                 %% password provided
                 {true,  _,    _,    _} -> {U, list_to_binary(Pass)};
                 %% password not provided, TLS certificate is present,
                 %% rabbitmq_mqtt.ssl_cert_login is true
                 {false, _, _, true}    -> {U, none};
                 %% anonymous, default password is configured
                 {false, true, true, _} -> {U, DefaultPass};
                 _                      -> {U, none}
            end
    end.

supported_subs_qos(?QOS_0) -> ?QOS_0;
supported_subs_qos(?QOS_1) -> ?QOS_1;
supported_subs_qos(?QOS_2) -> ?QOS_1.

delivery_mode(?QOS_0) -> 1;
delivery_mode(?QOS_1) -> 2.

%% different qos subscriptions are received in different queues
%% with appropriate durability and timeout arguments
%% this will lead to duplicate messages for overlapping subscriptions
%% with different qos values - todo: prevent duplicates
ensure_queue(Qos, #proc_state{ channels      = {Channel, _},
                               client_id     = ClientId,
                               clean_sess    = CleanSess,
                          consumer_tags = {TagQ0, TagQ1} = Tags} = PState) ->
    {QueueQ0, QueueQ1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    SubscriptionTTL = rabbit_mqtt_util:env(subscription_ttl),
    Qos1Args = case {SubscriptionTTL, CleanSess} of
                   {undefined, _} ->
                       [];
                   {Ms, false} when is_integer(Ms) ->
                       [{<<"x-expires">>, long, Ms}];
                   _ ->
                       []
               end,
    Qos0Args = case CleanSess of
                   false -> Qos0Args1 = case is_integer(SubscriptionTTL) of
                                            true -> [{<<"x-expires">>, long, SubscriptionTTL}];
                                            false -> []
                                        end,
                            lists:append( [{<<"x-message-ttl">>, long, 0}], Qos0Args1);
                   _ -> []
               end,
    QueueSetup =
        case {TagQ0, TagQ1, Qos} of
            {undefined, _, ?QOS_0} ->
                {QueueQ0,
                 #'queue.declare'{ queue       = QueueQ0,
                                   durable     = false,
                                   auto_delete = CleanSess,
                                   arguments   = Qos0Args},
                 #'basic.consume'{ queue  = QueueQ0,
                                   no_ack = true }};
            {_, undefined, ?QOS_1} ->
                {QueueQ1,
                 #'queue.declare'{ queue       = QueueQ1,
                                   durable     = true,
                                   %% Clean session means a transient connection,
                                   %% translating into auto-delete.
                                   %%
                                   %% see rabbitmq/rabbitmq-mqtt#37
                                   auto_delete = CleanSess,
                                   arguments   = Qos1Args },
                 #'basic.consume'{ queue  = QueueQ1,
                                   no_ack = false }};
            {_, _, ?QOS_0} ->
                {exists, QueueQ0};
            {_, _, ?QOS_1} ->
                {exists, QueueQ1}
          end,
    case QueueSetup of
        {Queue, Declare, Consume} ->
            #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
            #'basic.consume_ok'{ consumer_tag = Tag } =
                amqp_channel:call(Channel, Consume),
            {Queue, PState #proc_state{ consumer_tags = setelement(Qos+1, Tags, Tag) }};
        {exists, Q} ->
            {Q, PState}
    end.

relay_status_for_disconnect(PState) ->
    case PState of 
            #proc_state{ auth_state = #auth_state{ username = Username }, client_id = ClientId } ->
                relay_client_status(ClientId, Username, 0);
            _ -> void
    end.

send_will(PState = #proc_state{ will_msg = WillMsg }) ->
    amqp_pub(WillMsg, PState).

http_relay(Path, Request, HttpOptions) ->
    case rabbit_mqtt_util:http_get(Path, Request, HttpOptions) of
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            case Code of
                200 -> ok;
                _ -> rabbit_log:log(connection, error, "relay status error ~p ~p ~n", [Code, Body])
            end;
        {error, _} = E -> rabbit_log:log(connection, error, "relay status error ~p ~n", [E])
    end.

relay_client_status(ClientId, Username, Status) ->
    [{path, Path}, {conn_timeout, ConnTimeout}, {timeout, Timeout}] = rabbit_mqtt_util:env(relay_status_backend_http),
    Request = [{username, Username},{clientId, ClientId}, {status, Status}],
    HttpOptions = [{timeout, Timeout},{connect_timeout, ConnTimeout},{version, "HTTP/1.0"}],
    spawn(rabbit_mqtt_processor, http_relay, [Path, Request, HttpOptions]).


amqp_pub(undefined, PState) ->
    PState;

%% set up a qos1 publishing channel if necessary
%% this channel will only be used for publishing, not consuming
amqp_pub(Msg   = #mqtt_msg{ qos = ?QOS_1 },
         PState = #proc_state{ channels       = {ChQos0, undefined},
                               awaiting_seqno = undefined,
                               connection     = Conn }) ->
    {ok, Channel} = amqp_connection:open_channel(Conn),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Channel, self()),
    amqp_pub(Msg, PState #proc_state{ channels       = {ChQos0, Channel},
                                      awaiting_seqno = 1 });

amqp_pub(#mqtt_msg{ qos        = Qos,
                    topic      = Topic,
                    dup        = Dup,
                    message_id = MessageId,
                    payload    = Payload },
         PState = #proc_state{ channels       = {ChQos0, ChQos1},
                               exchange       = Exchange,
                               unacked_pubs   = UnackedPubs,
                               awaiting_seqno = SeqNo }) ->
    Method = #'basic.publish'{ exchange    = Exchange,
                               routing_key =
                                   rabbit_mqtt_util:mqtt2amqp(Topic)},
    Headers = [{<<"x-mqtt-publish-qos">>, byte, Qos},
               {<<"x-mqtt-dup">>, bool, Dup}],
    Props = case Qos of
        ?QOS_0 -> #'P_basic'{ headers       = Headers,
                              delivery_mode = delivery_mode(Qos),
                              expiration = <<"0">>};
        _ -> #'P_basic'{ headers       = Headers,
                         delivery_mode = delivery_mode(Qos)}
    end,
    Msg = #amqp_msg{ props   = Props,
                     payload = Payload },
    {UnackedPubs1, Ch, SeqNo1} =
        case Qos =:= ?QOS_1 andalso MessageId =/= undefined of
            true  -> {gb_trees:enter(SeqNo, MessageId, UnackedPubs), ChQos1,
                      SeqNo + 1};
            false -> {UnackedPubs, ChQos0, SeqNo}
        end,
    amqp_channel:cast_flow(Ch, Method, Msg),
    PState #proc_state{ unacked_pubs   = UnackedPubs1,
                        awaiting_seqno = SeqNo1 }.

adapter_info(Sock, ProtoName) ->
    amqp_connection:socket_adapter_info(Sock, {ProtoName, "N/A"}).

set_proto_version(AdapterInfo = #amqp_adapter_info{protocol = {Proto, _}}, Vsn) ->
    AdapterInfo#amqp_adapter_info{protocol = {Proto,
        human_readable_mqtt_version(Vsn)}}.

human_readable_mqtt_version(3) ->
    "3.1.0";
human_readable_mqtt_version(4) ->
    "3.1.1";
human_readable_mqtt_version(_) ->
    "N/A".

send_client(Frame, PState = #proc_state{ socket = Sock }) ->
    packet_log(Frame, PState),
    try rabbit_net:port_command(Sock, rabbit_mqtt_frame:serialise(Frame))
    catch
        error:Reason -> self() ! {inet_reply, Sock, {error, Reason}}
    end.


close_connection_and_relay(PState = #proc_state{ connection = undefined }) ->
    PState;
close_connection_and_relay(PState = #proc_state{ connection = _Connection,
                                                 client_id  = _ClientId }) ->
    try relay_status_for_disconnect(PState)
    catch
        _:EndErrorReason -> rabbit_log:error("~p ~p ~p ~n", [PState, EndErrorReason, erlang:get_stacktrace()])
    end,
    
    close_connection(PState).

close_connection(PState = #proc_state{ connection = Connection,
                                       client_id  = ClientId }) ->
    try send_will(PState)
    catch
        _:EndErrorReason -> rabbit_log:error("~p ~p ~p ~n", [PState, EndErrorReason, erlang:get_stacktrace()])
    end,
    
    % todo: maybe clean session
    case ClientId of
        undefined -> ok;
        _         -> ok = rabbit_mqtt_collector:unregister(ClientId, self())
    end,
    %% ignore noproc or other exceptions to avoid debris
    try amqp_connection:close(Connection)
    catch
        _:Reason -> rabbit_log:error("~p ~p", [PState, Reason])
    end,
    PState #proc_state{ channels   = {undefined, undefined},
                        connection = undefined }.

% NB: check_*_or_die: MQTT spec says we should ack normally, ie pretend there
% was no auth error, but here we are closing the connection with an error. This
% is what happens anyway if there is an authorization failure at the AMQP level.

check_publish_or_die(TopicName, Fn, PState) ->
  case check_topic_access(TopicName, write, PState) of
    ok -> Fn();
    _ -> {err, unauthorized, PState}
  end.

check_subscribe_or_die([], Fn, _) ->
  Fn();

check_subscribe_or_die([#mqtt_topic{name = TopicName} | Topics], Fn, PState) ->
  case check_topic_access(TopicName, read, PState) of
    ok -> check_subscribe_or_die(Topics, Fn, PState);
    _ -> {err, unauthorized, PState}
  end.

check_topic_access(TopicName, Access,
                   #proc_state{
                      auth_state = #auth_state{user = User,
                                               vhost = VHost}}) ->
  Resource = #resource{virtual_host = VHost,
                       kind = topic,
                       name = TopicName},
  rabbit_access_control:check_resource_access(User, Resource, Access).
