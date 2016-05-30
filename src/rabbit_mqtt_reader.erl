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

-module(rabbit_mqtt_reader).
-behaviour(gen_server2).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-export([conserve_resources/3, start_keepalive/2]).

-export([ssl_login_name/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt.hrl").

%%----------------------------------------------------------------------------

start_link(KeepaliveSup, Ref, Sock) ->
    Pid = proc_lib:spawn_link(?MODULE, init,
                              [[KeepaliveSup, Ref, Sock]]),

    %% In the event that somebody floods us with connections, the
    %% reader processes can spew log events at error_logger faster
    %% than it can keep up, causing its mailbox to grow unbounded
    %% until we eat all the memory available and crash. So here is a
    %% meaningless synchronous call to the underlying gen_event
    %% mechanism. When it returns the mailbox is drained, and we
    %% return to our caller to accept more connections.
    gen_event:which_handlers(error_logger),

    {ok, Pid}.

conserve_resources(Pid, _, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Conserve},
    ok.

%%----------------------------------------------------------------------------

init([KeepaliveSup, Ref, Sock]) ->
    process_flag(trap_exit, true),
    rabbit_net:accept_ack(Ref, Sock),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            log(debug, "MQTT accepting TCP connection ~p (~s)~n", [self(), ConnStr]),
            rabbit_alarm:register(
              self(), {?MODULE, conserve_resources, []}),
            ProcessorState = rabbit_mqtt_processor:initial_state(Sock,ssl_login_name(Sock)),
            erlang:send_after(rabbit_mqtt_util:env(connection_idle_timeout), self(), connection_idle_timeout),
            gen_server2:enter_loop(?MODULE, [],
             rabbit_event:init_stats_timer(
              control_throttle(
               #state{socket                 = Sock,
                      conn_name              = ConnStr,
                      await_recv             = false,
                      connection_state       = connected,
                      received_connect_frame = false,
                      keepalive              = {none, none},
                      keepalive_sup          = KeepaliveSup,
                      conserve               = false,
                      parse_state            = rabbit_mqtt_frame:initial_state(),
                      proc_state             = ProcessorState }), #state.stats_timer),
             {backoff, 1000, 1000, 10000});
        {network_error, Reason} ->
            rabbit_net:fast_close(Sock),
            terminate({shutdown, Reason}, undefined);
        {error, enotconn} ->
            rabbit_net:fast_close(Sock),
            terminate(shutdown, undefined);
        {error, Reason} ->
            rabbit_net:fast_close(Sock),
            terminate({network_error, Reason}, undefined)
    end.

handle_call(Msg, From, State) ->
    {stop, {mqtt_unexpected_call, Msg, From}, State}.

handle_cast(duplicate_id,
            State = #state{ proc_state = PState,
                            conn_name  = ConnName }) ->
    log(warning, "MQTT disconnecting duplicate client id ~p (~p)~n",
                 [rabbit_mqtt_processor:info(client_id, PState), ConnName]),
    {stop, {shutdown, duplicate_id}, State};

handle_cast(Msg, State) ->
    {stop, {mqtt_unexpected_cast, Msg}, State}.

handle_info({#'basic.deliver'{}, #amqp_msg{}, _DeliveryCtx} = Delivery,
            State = #state{ proc_state = ProcState }) ->
    callback_reply(State, rabbit_mqtt_processor:amqp_callback(Delivery,
                                                              ProcState));

handle_info(#'basic.ack'{} = Ack, State = #state{ proc_state = ProcState }) ->
    callback_reply(State, rabbit_mqtt_processor:amqp_callback(Ack, ProcState));

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};

handle_info(#'basic.cancel'{}, State) ->
    {stop, {shutdown, subscription_cancelled}, State};

handle_info({'EXIT', _Conn, Reason}, State) ->
    {stop, {connection_died, Reason}, State};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}},
            State = #state{ socket = Sock, connection_state = blocked }) ->
    {noreply, State#state{ deferred_recv = Data }, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}},
            State = #state{ socket = Sock, connection_state = ConnectionState }) ->
    NextConnectionState = 
        case ConnectionState of
            connected -> running ;
            _ -> ConnectionState
        end,
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false,
                                           connection_state = NextConnectionState }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State = #state {}) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State = #state {}) ->
    network_error(Reason, State);

handle_info({conserve_resources, Conserve}, State) ->
    maybe_process_deferred_recv(
        control_throttle(State #state{ conserve = Conserve }));

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    maybe_process_deferred_recv(control_throttle(State));

handle_info({start_keepalives, Keepalive},
            State = #state { keepalive_sup = KeepaliveSup, socket = Sock }) ->
    %% Only the client has the responsibility for sending keepalives
    SendFun = fun() -> ok end,
    Parent = self(),
    ReceiveFun = fun() -> Parent ! keepalive_timeout end,
    Heartbeater = rabbit_heartbeat:start(
                    KeepaliveSup, Sock, 0, SendFun, Keepalive, ReceiveFun),
    {noreply, State #state { keepalive = Heartbeater }};

handle_info(keepalive_timeout, State = #state {conn_name = ConnStr,
                                               proc_state = _PState}) ->
    log(error, "closing MQTT connection ~p (keepalive timeout)~n", [ConnStr]),
    {stop, {shutdown, keepalive_timeout}, State};

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};

handle_info(connection_idle_timeout, State = #state{connection_state = ConnectionState}) ->
    case ConnectionState of
        running ->
            {noreply, State, hibernate};
        _ ->
            {stop, {connection_idle_timeout, "connection_idle_timeout"}, State}
    end;

handle_info(Msg, State) ->
    {stop, {mqtt_unexpected_msg, Msg}, State}.

terminate(Reason, State) ->
    maybe_emit_stats(State),
    do_terminate(Reason, State).

do_terminate({network_error, {ssl_upgrade_error, closed}, ConnStr}, _State) ->
    log(error, "MQTT detected TLS upgrade error on ~s: connection closed~n",
       [ConnStr]);

do_terminate({network_error,
           {ssl_upgrade_error,
            {tls_alert, "handshake failure"}}, ConnStr}, _State) ->
    log(error, "MQTT detected TLS upgrade error on ~s: handshake failure~n",
       [ConnStr]);

do_terminate({network_error,
           {ssl_upgrade_error,
            {tls_alert, "unknown ca"}}, ConnStr}, _State) ->
    log(error, "MQTT detected TLS certificate verification error on ~s: alert 'unknown CA'~n",
       [ConnStr]);

do_terminate({network_error,
           {ssl_upgrade_error,
            {tls_alert, Alert}}, ConnStr}, _State) ->
    log(error, "MQTT detected TLS upgrade error on ~s: alert ~s~n",
       [ConnStr, Alert]);

do_terminate({network_error, {ssl_upgrade_error, Reason}, ConnStr}, _State) ->
    log(error, "MQTT detected TLS upgrade error on ~s: ~p~n",
        [ConnStr, Reason]);

do_terminate({network_error, Reason, ConnStr}, _State) ->
    log(error, "MQTT detected network error on ~s: ~p~n",
        [ConnStr, Reason]);

do_terminate({network_error, Reason}, _State) ->
    log(error, "MQTT detected network error: ~p~n", [Reason]);

do_terminate(normal, #state{proc_state = ProcState,
                         conn_name  = ConnName}) ->
    rabbit_mqtt_processor:close_connection_and_relay(ProcState),
    log(info, "closing MQTT connection ~p (~s)~n", [self(), ConnName]),
    ok;

do_terminate(Reason, #state{proc_state = ProcState,
                            conn_name  = ConnName}) ->
    case Reason of
        {shutdown,duplicate_id} ->
            rabbit_mqtt_processor:close_connection(ProcState);
        _ ->
            rabbit_mqtt_processor:close_connection_and_relay(ProcState)
    end,
    log(error, "closing MQTT connection ~p (~s) Reason:~p ~n", [self(), ConnName, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

ssl_login_name(Sock) ->
  case rabbit_net:peercert(Sock) of
      {ok, C}              -> case rabbit_ssl:peer_cert_auth_name(C) of
                                    unsafe    -> none;
                                    not_found -> none;
                                    Name      -> Name
                                end;
      {error, no_peercert} -> none;
      nossl                -> none
  end.

%%----------------------------------------------------------------------------

log_new_connection(#state{conn_name = ConnStr}) ->
    log(info, "accepting MQTT connection ~p (~s)~n", [self(), ConnStr]).

process_received_bytes(<<>>, State = #state{proc_state = ProcState,
                                            received_connect_frame = false}) ->
    MqttConn = ProcState#proc_state.connection,
    case MqttConn of
        undefined -> ok;
        _         -> log_new_connection(State)
    end,
    {noreply, ensure_stats_timer(State#state{ received_connect_frame = true }), hibernate};
process_received_bytes(<<>>, State) ->
    {noreply, ensure_stats_timer(State), hibernate};
process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case rabbit_mqtt_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {noreply,
             ensure_stats_timer(control_throttle( State #state{ parse_state = ParseState1 })),
             hibernate};
        {ok, Frame, Rest} ->
            case rabbit_mqtt_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1, ConnPid} ->
                    PS = rabbit_mqtt_frame:initial_state(),
                    process_received_bytes(
                      Rest,
                      State #state{ parse_state = PS,
                                    proc_state = ProcState1,
                                    connection = ConnPid });
                {error, Reason, ProcState1} ->
                    log(info, "MQTT protocol error ~p for connection ~p~n",
                        [Reason, ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                {error, Error} ->
                    log(error, "MQTT detected framing error '~p' for connection ~p~n",
                        [Error, ConnStr]),
                    {stop, {shutdown, Error}, State};
                {stop, ProcState1} ->
                    {stop, normal, pstate(State, ProcState1)}
            end;
        {error, Error} ->
            log(error, "MQTT detected framing error '~p' for connection ~p~n",
                [ConnStr, Error]),
            {stop, {shutdown, Error}, State}
    end.

callback_reply(State, {ok, ProcState}) ->
    {noreply, pstate(State, ProcState), hibernate};
callback_reply(State, {error, Reason, ProcState}) ->
    {stop, Reason, pstate(State, ProcState)}.

start_keepalive(_,   0        ) -> ok;
start_keepalive(Pid, Keepalive) -> Pid ! {start_keepalives, Keepalive}.

pstate(State = #state {}, PState = #proc_state{}) ->
    State #state{ proc_state = PState }.

%%----------------------------------------------------------------------------

log(Level, Fmt, Args) -> rabbit_log:log(connection, Level, Fmt, Args).

network_error(closed,
              State = #state{ conn_name  = ConnStr,
                              proc_state = PState }) ->
    MqttConn = PState#proc_state.connection,
    log(case MqttConn of
            undefined  -> debug;
            _          -> info
        end,
        "MQTT detected network error for ~p: peer closed TCP connection~n",
        [ConnStr]),
    {stop, {shutdown, conn_closed}, State};

network_error(Reason,
              State = #state{ conn_name  = ConnStr,
                              proc_state = _PState }) ->
    log(info, "MQTT detected network error for ~p: ~p~n", [ConnStr, Reason]),
    {stop, {shutdown, conn_closed}, State}.

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ deferred_recv = Data }) when Data =/= undefined ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    rabbit_net:async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

control_throttle(State = #state{ connection_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve orelse credit_flow:blocked()} of
        {running,   true} -> ok = rabbit_heartbeat:pause_monitor(
                                    State#state.keepalive),
                             State #state{ connection_state = blocked };
        {connected,   true} -> ok = rabbit_heartbeat:pause_monitor(
                                    State#state.keepalive),
                             State #state{ connection_state = blocked };
        {blocked,  false} -> ok = rabbit_heartbeat:resume_monitor(
                                    State#state.keepalive),
                             run_socket(State #state{
                                                connection_state = running });
        {_,            _} -> run_socket(State)
    end.

maybe_process_deferred_recv(State = #state{ deferred_recv = undefined }) ->
    {noreply, State, hibernate};
maybe_process_deferred_recv(State = #state{ deferred_recv = Data, socket = Sock }) ->
    handle_info({inet_async, Sock, noref, {ok, Data}},
                State#state{ deferred_recv = undefined }).

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                            fun() -> emit_stats(State) end).

emit_stats(State=#state{socket=Sock, connection_state=ConnState, connection=Conn}) ->
    RealConnStat = case ConnState of
                       connected -> running;
                       _         -> ConnState
                   end,
    SockInfos = case rabbit_net:getstat(Sock,
            [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]) of
        {ok,    SI} -> SI;
        {error,  _} -> []
    end,
    Infos = [{pid, Conn}, {state, RealConnStat}|SockInfos],
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1).

ensure_stats_timer(State = #state{}) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).
