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
%% Copyright (c) 2016 WorksMobile, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_ets_for_cluster).

-behaviour(rabbit_mqtt_retained_msg_store).
-include("rabbit_mqtt.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1, all/1]).

-define(RESOURCE_TYPE, retained_msg).

-record(store_state, {
  %% ETS table ID
  table,
  %% where the table is stored on disk
  filename
}).

new(Dir, VHost) ->
  Path = rabbit_mqtt_util:path_for(Dir, VHost),
  TableName = rabbit_mqtt_retained_msg_store:table_name_for(VHost),
  file:delete(Path),
  Tid = ets:new(TableName, [set, public, {keypos, #retained_message.topic}]),
  #store_state{table = Tid, filename = Path}.

internal_recover(Dir, VHost, []) ->
  Path = rabbit_mqtt_util:path_for(Dir, VHost),
  case ets:file2tab(Path) of
    {ok, Tid}  -> file:delete(Path),
                  {ok, #store_state{table = Tid, filename = Path}};
    {error, _} -> {error, uninitialized}
  end;
internal_recover(Dir, VHost, [Pid | Tails]) ->
  try 
      Messages = gen_server2:call(Pid, {sync}, rabbit_mqtt_util:env(retained_sync_timeout)),
      #store_state{table = Tid, filename = Path} = new(Dir, VHost),
      true = ets:insert(Tid, Messages),
      {ok, #store_state{table = Tid, filename = Path}}
  catch
    _:Reason -> 
        case Tails of
            [] -> rabbit_log:error("MQTT retained message sync error: ~w", [Reason]),
                  throw(Reason);
            _  -> internal_recover(Dir, VHost, Tails)
        end
  end.

recover(Dir, VHost) ->
  resource_discovery:add_local_resource_tuple({?RESOURCE_TYPE, self()}),
  resource_discovery:add_target_resource_type(?RESOURCE_TYPE),
  resource_discovery:sync_resources(),
  Pids = resource_discovery:get_resources(?RESOURCE_TYPE),
  rabbit_log:info("MQTT retained message pids: ~p", [Pids]),
  internal_recover(Dir, VHost, lists:delete(self(), Pids)).

multi_cast([], _Msg) -> ok;
multi_cast([Pid | Tails], Msg) ->
    gen_server2:cast(Pid, Msg),
    multi_cast(Tails, Msg).

insert(Topic, Msg, #store_state{table = T}) ->
  true = ets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}),
  multi_cast(lists:delete(self(), resource_discovery:get_resources(?RESOURCE_TYPE)),
             {retain, Topic, Msg}),
  ok.

lookup(Topic, #store_state{table = T}) ->
  case ets:lookup(T, Topic) of
    []      -> not_found;
    [Entry] -> Entry
  end.

delete(Topic, #store_state{table = T}) ->
  true = ets:delete(T, Topic),
  multi_cast(lists:delete(self(), resource_discovery:get_resources(?RESOURCE_TYPE)),
             {clear, Topic}),
  ok.

terminate(#store_state{table = T, filename = Path}) ->
  resource_discovery:delete_local_resource_tuple({retained_msg, node()}),
  resource_discovery:trade_resources(),
  ok = ets:tab2file(T, Path,
                    [{extended_info, [object_count]}]).

all(#store_state{table = T}) -> ets:tab2list(T).
