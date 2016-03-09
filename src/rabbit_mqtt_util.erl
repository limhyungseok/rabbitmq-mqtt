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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_util).

-include("rabbit_mqtt.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

subcription_queue_name(ClientId) ->
    Base = "mqtt-subscription-" ++ ClientId ++ "qos",
    {list_to_binary(Base ++ "0"), list_to_binary(Base ++ "1")}.

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator
mqtt2amqp(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(re:replace(Topic, "[\.]", "\\\\.", [global]),
                            "/", ".", [global]),
                 "[\+]", "*", [global])).

amqp2mqtt(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(re:replace(Topic, "[\*]", "+", [global]),
                            "[\.]", "/", [global]),
                 "\\\\/", ".", [global])).

gen_client_id() ->
    lists:nthtail(1, rabbit_guid:string(rabbit_guid:gen_secure(), [])).

env(Key) ->
    case application:get_env(rabbitmq_mqtt, Key) of
        {ok, Val} -> coerce_env_value(Key, Val);
        undefined -> undefined
    end.

coerce_env_value(default_pass, Val) -> to_binary(Val);
coerce_env_value(default_user, Val) -> to_binary(Val);
coerce_env_value(exchange, Val)     -> to_binary(Val);
coerce_env_value(vhost, Val)        -> to_binary(Val);
coerce_env_value(_, Val)            -> Val.

to_binary(Val) when is_list(Val) -> list_to_binary(Val);
to_binary(Val)                   -> Val.

table_lookup(undefined, _Key) ->
    undefined;
table_lookup(Table, Key) ->
    rabbit_misc:table_lookup(Table, Key).

vhost_name_to_dir_name(VHost) ->
    vhost_name_to_dir_name(VHost, ".ets").
vhost_name_to_dir_name(VHost, Suffix) ->
    <<Num:128>> = erlang:md5(VHost),
    "mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num]) ++ Suffix.

path_for(Dir, VHost) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost)).

path_for(Dir, VHost, Suffix) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost, Suffix)).


vhost_name_to_table_name(VHost) ->
  <<Num:128>> = erlang:md5(VHost),
  list_to_atom("rabbit_mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num])).

http_post(Path, Request) ->
    URI = uri_parser:parse(Path, [{port, 80}]),
    {host, Host} = lists:keyfind(host, 1, URI),
    {port, Port} = lists:keyfind(port, 1, URI),
    HostHdr = rabbit_misc:format("~s:~b", [Host, Port]),
    httpc:request(post, 
                  {Path, [{"Host", HostHdr}], "application/x-www-form-urlencoded", mochiweb_util:urlencode(Request)}, 
                  [{version, "HTTP/1.0"}], []).


http_get(Path, Request, HTTPOptions) ->
    GET_URI = Path ++ "?" ++ mochiweb_util:urlencode(Request),
    URI = uri_parser:parse(GET_URI, [{port, 80}]),
    {host, Host} = lists:keyfind(host, 1, URI),
    {port, Port} = lists:keyfind(port, 1, URI),
    HostHdr = rabbit_misc:format("~s:~b", [Host, Port]),
    httpc:request(get, {GET_URI, [{"Host", HostHdr}]}, HTTPOptions, []).

get_running_nodes() ->
    S = rabbit_mnesia:status(),
    Running = proplists:get_value(running_nodes, S),
    lists:filter(fun (Node) 
                    when Node =:= node() -> false;
                    (_) -> true
                 end, Running).
