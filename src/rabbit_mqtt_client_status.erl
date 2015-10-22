-module(rabbit_mqtt_client_status).

-define(ENCODING, utf8).

-include("rabbit_mqtt.hrl").

-compile(export_all).

open_table() -> 
    rabbit_log:info("open_table ~n", []),
    ets:new(client_status_table, [public,named_table,{keypos, #client_status.client_id}]).

close_table() -> 
    rabbit_log:info("close_table ~n", []).

insert(ClientId, Status) ->
    ets:insert(client_status_table, #client_status{client_id = ClientId,
                                                   status = Status}).

lookup(ClientId) ->
    case ets:lookup(client_status_table, ClientId) of 
        []  -> not_found;
        [Entry] -> Entry
    end.

delete(ClientId) ->
    ets:delete(client_status_table, ClientId).