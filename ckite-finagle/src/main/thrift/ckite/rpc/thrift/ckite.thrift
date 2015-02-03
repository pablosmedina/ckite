namespace java ckite.rpc.thrift

struct LogEntryST {
	1: required i32 term;
	2: required i64 index;
	3: required binary command;
}

struct AppendEntriesST {
	1: required i32 term;
	2: required string leaderId; 
	3: optional i64 commitIndex = -1; 
	4: optional i64 prevLogIndex = -1;
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
	3: optional i64 lastLogIndex = -1;
	4: optional i32 lastLogTerm = -1; 
}

struct RequestVoteResponseST {
	1: required i32 currentTerm;
	2: required bool granted;
}

struct SnapshotST {
	1: required binary stateMachineState;
	2: required i64 lastLogEntryIndex;
	3: required i32 lastLogEntryTerm;
	4: required binary membershipState;
}

struct InstallSnapshotST {
	1: required i32 term;
	2: required string leaderId;
	3: required SnapshotST snapshot;
}

struct InstallSnapshotResponseST {
	1: required bool success;
}

struct JoinMemberST {
	1: required string memberId;
}

struct JoinMemberResponseST {
	1: required bool success;
}

service CKiteService {

	RequestVoteResponseST sendRequestVote(1:RequestVoteST requestVote);

	AppendEntriesResponseST sendAppendEntries(1:AppendEntriesST appendEntries);
	
	binary sendCommand(1:binary command);
	
	InstallSnapshotResponseST sendInstallSnapshot(1:InstallSnapshotST installSnapshot);
	
	JoinMemberResponseST sendJoinMember(1:JoinMemberST memberId);
	
}
