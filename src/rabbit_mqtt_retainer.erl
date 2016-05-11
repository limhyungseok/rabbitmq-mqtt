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

-module(rabbit_mqtt_retainer).

-behaviour(gen_server2).
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_frame.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, start_link/2]).

-export([retain/3, fetch/2, clear/2, store_module/0]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 30000).
-define(RESOURCE_TYPE, retained_msg).

-record(retainer_state, {store_mod,
                         store,
                         sync}).

-ifdef(use_specs).

-spec(retain/3 :: (pid(), string(), mqtt_msg()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}).

-endif.

%%----------------------------------------------------------------------------

start_link(RetainStoreMod, VHost) ->
    gen_server2:start_link(?MODULE, [RetainStoreMod, VHost], []).

retain(Pid, Topic, Msg = #mqtt_msg{retain = true}) ->
    Message = {retain, Topic, Msg},
    relay_others(Pid, Message),
    gen_server2:cast(Pid, Message);

retain(_Pid, _Topic, Msg = #mqtt_msg{retain = false}) ->
    throw({error, {retain_is_false, Msg}}).

fetch(Pid, Topic) ->
    gen_server2:call(Pid, {fetch, Topic}, ?TIMEOUT).

clear(Pid, Topic) ->
    Message = {clear, Topic},
    relay_others(Pid, Message),
    gen_server2:cast(Pid, Message).

relay_others(ExceptPid, Message) ->
    case rabbit_mqtt_util:env(retained_sync_cluster) of
        true -> 
            lists:foreach(fun (Pid) 
                                when ExceptPid =:= Pid -> void ;
                                (Pid) -> gen_server2:cast(Pid, Message)
                          end, resource_discovery:get_resources(?RESOURCE_TYPE));
        false -> void
    end.

%%----------------------------------------------------------------------------

init([StoreMod, VHost]) ->
    process_flag(trap_exit, true),
    Sync = rabbit_mqtt_util:env(retained_sync_cluster),
    Recover = case Sync of
                true ->
                    resource_discovery:add_local_resource_tuple({?RESOURCE_TYPE, self()}),
                    resource_discovery:add_target_resource_type(?RESOURCE_TYPE),
                    resource_discovery:sync_resources(),
                    case sync_module(lists:delete(self(), resource_discovery:get_resources(?RESOURCE_TYPE)), 
                                     StoreMod, VHost) of 
                        {ok, Store} -> Store;
                        _ -> true
                    end;
                false -> true
              end,
    State = case Recover of 
                true -> case StoreMod:recover(store_dir(), VHost) of
                            {ok, Store0} -> #retainer_state{store = Store0,
                                                            store_mod = StoreMod,
                                                            sync = Sync};
                            {error, _}  -> #retainer_state{store = StoreMod:new(store_dir(), VHost),
                                                           store_mod = StoreMod,
                                                           sync = Sync}
                        end;
                Store1 -> #retainer_state{store = Store1, store_mod = StoreMod, sync = Sync}
            end,
    {ok, State}.

store_module() ->
    case application:get_env(rabbitmq_mqtt, retained_message_store) of
        {ok, Mod} -> Mod;
        undefined -> undefined
    end.

sync_module([], _StoreMod, _VHost) -> {error, "can't sync retained store"};
sync_module([Pid | Tails], StoreMod, VHost) ->
    try
        Messages = gen_server2:call(Pid, {sync}, rabbit_mqtt_util:env(retained_sync_timeout)),
        Store = StoreMod:new(store_dir(), VHost, Messages),
        {ok, Store}
    catch
        _:Reason ->
            rabbit_log:error("MQTT retained message sync error: ~w", [Reason]),
            sync_module(Tails, StoreMod, VHost)
    end.

%%----------------------------------------------------------------------------

handle_cast({retain, Topic, Msg},
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    ok = Mod:insert(Topic, Msg, Store),
    {noreply, State};
handle_cast({clear, Topic},
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    ok = Mod:delete(Topic, Store),
    {noreply, State}.

handle_call({sync}, From, 
            State = #retainer_state{store = Store, store_mod = Mod}) ->
    rabbit_log:info("MQTT retained message sync from:~w", [From]),
    {reply, Mod:all(Store), State};

handle_call({fetch, Topic}, _From,
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    Reply = case Mod:lookup(Topic, Store) of
                #retained_message{mqtt_msg = Msg} -> Msg;
                not_found                         -> undefined
            end,
    {reply, Reply, State}.

handle_info(stop, State) ->
    {stop, normal, State};

handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

store_dir() ->
    rabbit_mnesia:dir().

terminate(_Reason, #retainer_state{store = Store, store_mod = Mod, sync = true}) ->
    resource_discovery:delete_local_resource_tuple({?RESOURCE_TYPE, self()}),
    resource_discovery:trade_resources(),
    Mod:terminate(Store),
    ok;

terminate(_Reason, #retainer_state{store = Store, store_mod = Mod, sync = false}) ->
    Mod:terminate(Store),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
