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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_mnesia).

-behaviour(rabbit_mqtt_retained_msg_store).
-include("rabbit_mqtt.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

another_nodes(Mynode, [Mynode | Tail], Count) -> lists:sublist(Tail, Count);
another_nodes(Mynode, [Node | Tail], Count) -> 
    AnotherNodes = another_nodes(Mynode, Tail, Count),
    case length(AnotherNodes) =:= Count of
        true -> AnotherNodes;
        false -> [Node | AnotherNodes]
    end;
another_nodes(_, [], _) -> [].

new(_, _) ->
  case mnesia:create_table(retained_message, [{attributes, record_info(fields, retained_message)},
                                              {disc_copies, [node() | another_nodes(node(), mnesia:system_info(db_nodes), 2)]}]) of
    {atomic, ok} ->
      mnesia:add_table_copy(retained_message, node(), disc_copies),
      {ok, "create table"};
    {aborted, Reason} -> 
      rabbit_log:error("create_table : aborted(~p) Reason(~p) ~n", [aborted, Reason]),
      {error, uninitialized}
  end.

recover(_, _) ->
  case lists:member(retained_message,mnesia:system_info(tables)) of
      true -> rabbit_log:info("recover(_, _) true"),{ok, "already exist"};
      false -> rabbit_log:info("recover(_, _) false"),{error, uninitialized}
  end.

insert(Topic, Msg, _) ->
  mnesia:transaction(fun() -> mnesia:write(#retained_message{topic = Topic, mqtt_msg = Msg}) end),
  ok.

lookup(Topic, _ ) ->
  Fun = fun() ->
      Q = qlc:q([RM || RM <- mnesia:table(retained_message), RM#retained_message.topic == Topic]),
      qlc:e(Q)
    end,
  case mnesia:transaction(Fun) of
      {atomic, [Entry]} -> Entry;
      _ -> not_found
  end.

delete(Topic, _) ->
  mnesia:transaction(fun() -> mnesia:delete({retained_message, Topic}) end),
  ok.

terminate(_) -> ok.
