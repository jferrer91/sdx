-module(gms3).
-export([start/1, start/2]).
-define(timeout,1000).
-define(arghh,100).

start(Name) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Self) end).


init(Name, Master) ->
    leader(Name, Master, [], 0).

start(Name, Grp) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Grp, Self) end).

init(Name, Grp, Master) ->
    Self = self(),
    Grp ! {join, Self},
    receive
        {view, Leader, Slaves, N} ->
            Ref = erlang:monitor(process,Leader),
            Master ! joined,
            NewLast = {view, Leader, Slaves, N},
            slave(Name, Master, Leader, Slaves, Ref, N+1, NewLast)
            after ?timeout -> %%creo que va aqui
                Master ! {error, "no reply from leader"}
    end.

leader(Name, Master, Slaves, N) ->
    receive
        {mcast, Msg} ->
            bcast(Name, {msg, Msg, N}, Slaves),  %% Fem broadcasts a tots els slaves
            Master ! {deliver, Msg},             %% Enviem al worker el missatge que hem rebut
            leader(Name, Master, Slaves, N+1);
        {join, Peer} ->
            NewSlaves = lists:append(Slaves, [Peer]),
            bcast(Name, {view, self(), NewSlaves, N}, NewSlaves), %% Fem broadcast a tots els slaves incluint el nou
            leader(Name, Master, NewSlaves, N+1);                   %% Actualitzem els slaves
        stop ->
            ok;
        Error ->
            io:format("leader ~s: strange message ~w~n", [Name, Error])
    end.

bcast(Name, Msg, Nodes) ->
    lists:foreach(fun(Node) ->
        Node ! Msg,
        crash(Name, Msg)
    end,
    Nodes).

crash(Name, Msg) ->
    case rand:uniform(?arghh) of
    ?arghh ->
        io:format("leader ~s CRASHED: msg ~w~n", [Name, Msg]),
        exit(no_luck);
    _ ->
        ok
end.

slave(Name, Master, Leader, Slaves, Ref, N, Last) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},  %% Worker tells the leader that he wants to send a message.
            slave(Name, Master, Leader, Slaves, Ref, N, Last);
        {join, Peer} ->
            Leader ! {join, Peer}, %% A Worker wants to join the group
            slave(Name, Master, Leader, Slaves, Ref, N, Last);
        {msg, Msg, N} ->
            NewLast = {msg, Msg, N},  %% Save the last received message
            Master ! {deliver, Msg},  %% A message from the Leader arrives, we deliver to the worker
            slave(Name, Master, Leader, Slaves, Ref, N+1, NewLast);
        {msg, _, I} when I < N ->     %% Discard the message, sequence number less than N.
            slave(Name, Master, Leader, Slaves, Ref, N, Last);
        {'DOWN', _Ref, process, Leader, _Reason} ->
            election(Name, Master, Slaves, N, Last);
        {view, Leader, NewSlaves, N} ->             %% Update the view of slaves
            NewLast = {view, Leader, NewSlaves, N}, %% Save the last received message
            slave(Name, Master, Leader, NewSlaves, Ref, N+1, NewLast);
        {view, NewLeader, NewSlaves, N} ->
            erlang:demonitor(Ref, [flush]),
            NewRef = erlang:monitor(process, NewLeader),
            NewLast = {view, NewLeader, NewSlaves, N},
            slave(Name, Master, Leader, NewSlaves, NewRef, N+1, NewLast);
        {view, _, _, I} when I < N -> %% Discard the message, sequence number less than N.
            slave(Name, Master, Leader, Slaves, Ref, N, Last);
       stop ->
            ok;
        Error ->
            io:format("slave ~s: strange message ~w~n", [Name, Error])
    end.

election(Name, Master, Slaves, N, Last) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
          bcast(Name, Last, Rest),  %% Forward possible lost messages from the old leader
          bcast(Name,{view, Self, Rest, N}, Rest),
          leader(Name, Master, Rest, N+1);
        [NewLeader|Rest] ->
          NewRef = erlang:monitor(process, NewLeader),
          slave(Name, Master, NewLeader, Rest, NewRef, N, Last)
    end.
