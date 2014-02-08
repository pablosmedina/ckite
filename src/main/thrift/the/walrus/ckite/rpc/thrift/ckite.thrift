namespace java the.walrus.ckite.rpc.thrift

struct LogEntryST {
	1: required i32 term;
	2: required i32 index;
	3: required binary command;
}

struct AppendEntriesST {
	1: required i32 term;
	2: required string leaderId; 
	3: optional i32 commitIndex = -1; 
	4: optional i32 prevLogIndex = -1;
	5: optional i32 prevLogTerm = -1;
	6: optional list<LogEntryST> entries;
}

struct AppendEntriesResponseST {
	1: required i32 term;
	2: required bool success;
}

struct RequestVoteST {
	1: required string memberId;
	2: required i32 term;
	3: optional i32 lastLogIndex = -1;
	4: optional i32 lastLogTerm = -1; 
}

struct RequestVoteResponseST {
	1: required i32 currentTerm;
	2: required bool granted;
}

struct InstallSnapshotST {
	1: required binary stateMachineState;
	2: required i32 lastLogEntryIndex;
	3: required i32 lastLogEntryTerm;
	4: required binary membershipState;
}

struct JoinRequestST {
	1: required string memberId;
}

struct JoinResponseST {
	1: required bool success;
}

struct GetMembersResponseST {
	1: required bool success;
	2: required list<string> members;
}

service CKiteService {

	RequestVoteResponseST sendRequestVote(1:RequestVoteST requestVote);

	AppendEntriesResponseST sendAppendEntries(1:AppendEntriesST appendEntries);
	
	binary forwardCommand(1:binary command);
	
	bool installSnapshot(1:InstallSnapshotST installSnapshot);
	
	JoinResponseST join(1:JoinRequestST memberId);
	
	GetMembersResponseST getMembers();

}
