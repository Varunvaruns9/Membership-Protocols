/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 6
#define TPING 2
#define FORWARD_PINGERS 3

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    PING,
    ACK,
    PING_REQ,
};

/** 
 * Messages format
 * PING: MsgType + Pinger.Address + PayloadSize + Payload
 * ACK: MsgType + Acker.Address + Dest.Address + PayloadSize + Payload
 * PING-REQ: MsgType + Pinger.Address + Dest.Address + PayloadSize + Payload
 */

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: PayloadMember
 *
 * DESCRIPTION: Class for storing entries that are piggybacked with messages.
 */
class PayloadMember: public MemberListEntry {
public:
	// true if Newly added active node; false if failed node.
	bool status;
	PayloadMember(MemberListEntry mem, bool status)
	{
		this->id = mem.id;
		this->port = mem.port;
		this->heartbeat = mem.heartbeat;
		this->timestamp = mem.timestamp;
		this->status = status;
	}
	PayloadMember() : MemberListEntry()
	{
		this->status = 0;
	}
};

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	vector<PayloadMember> payload;
	vector<MemberListEntry>::iterator checkPos;
	char NULLADDR[6];

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	void refreshPayload();
	vector<PayloadMember>::iterator findPayload(PayloadMember pay);
	vector<MemberListEntry>::iterator findMember(MemberListEntry mem);
	void updateLists(char *curr);
	void pushPayload(char *msg);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
