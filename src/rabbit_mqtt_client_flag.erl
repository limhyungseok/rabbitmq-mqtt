-module(rabbit_mqtt_client_flag).

-define(ENCODING, utf8).

-include("rabbit_mqtt.hrl").

-compile(export_all).

open_table() -> 
    rabbit_log:info("open_table ~n", []),
    ets:new(client_flags, [public,named_table,{keypos, #client_flag.client_id}]).

close_table() -> 
    rabbit_log:info("close_table ~n", []).

insert(ClientId, Flag) ->
    ets:insert(client_flags, #client_flag{client_id = ClientId,
                                          flag = Flag}).

lookup(ClientId) ->
    case ets:lookup(client_flags, ClientId) of 
        []  -> not_found;
        [Entry] -> Entry
    end.

delete(ClientId) ->
    ets:delete(client_flags, ClientId).