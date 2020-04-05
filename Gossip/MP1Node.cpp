/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        // Add yourself to your member list
        int id = *(int*)(&memberNode->addr.addr);
        short port = *(short*)(&memberNode->addr.addr[4]);
        MemberListEntry self(id, port, memberNode->heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(self);

        log->logNodeAdd(&(memberNode->addr), &(memberNode->addr));
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Increase the heartbeat
    memberNode->heartbeat++;

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

// Function to convert an id and a port to an Address
Address idTOaddr(int id, short port)
{
    Address address;
    memcpy(&(address.addr[0]), &id, sizeof(int));
    memcpy(&(address.addr[4]), &port, sizeof(short));
    return address;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    Member *node = (Member *)env;

    if (((MessageHdr *)data)->msgType == JOINREQ)
    {
        // Add new node in coordinator's membership list
        Address address;
        long heartbeat;
        memcpy((char *)&address, (char *)((MessageHdr *)data+1), sizeof(Address));
        memcpy((char *)&heartbeat, (char *)((MessageHdr *)data+1) + 1 + sizeof(Address), sizeof(long));

        int id = *(int*)(address.addr);
        int port = *(short*)(address.addr+4);

        MemberListEntry newMember(id, port, heartbeat, par->getcurrtime());
        node->memberList.push_back(newMember);

        // Create a log for added node
        log->logNodeAdd(&(node->addr), &(address));

        // Create a JOINREP message for new node
        size_t sizeList = node->memberList.size();
        size_t msgsize = sizeof(MessageHdr) + sizeof(sizeList) + sizeList * sizeof(MemberListEntry);
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        msg->msgType = JOINREP;
        memcpy((char *)(msg+1), &(sizeList), sizeof(sizeList));
        char *curr = (char *)(msg+1) + sizeof(sizeList);
        for (auto entry: node->memberList)
        {
            memcpy(curr, (char *)&(entry), sizeof(entry));
            curr = curr + sizeof(entry);
        }

        // Send JOINREP to newly added node
        emulNet->ENsend(&(node->addr), &address, (char *)msg, msgsize);

        free(msg);
    }
    else if (((MessageHdr *)data)->msgType == JOINREP)
    {
        node->inGroup = 1;

        // Add all elements to your membership list
        size_t sizeList;
        char *curr = (char *)((MessageHdr *)data+1);

        memcpy(&sizeList, curr, sizeof(sizeList));
        curr = curr + sizeof(sizeList);

        MemberListEntry newEntry;
        for (int i=0;i<sizeList;++i)
        {
            memcpy(&newEntry, curr, sizeof(newEntry));
            curr = curr + sizeof(newEntry);
            node->memberList.push_back(newEntry);

            Address address = idTOaddr(newEntry.id, newEntry.port);

            log->logNodeAdd(&(node->addr), &(address));
        }
    }
    else if (((MessageHdr *)data)->msgType == HBEAT)
    {
        size_t sizeList;
        char *curr = (char *)((MessageHdr *)data+1);

        memcpy((char *)&sizeList, curr, sizeof(sizeList));
        curr = curr + sizeof(sizeList);

        MemberListEntry newEntry;
        newEntry.timestamp = par->getcurrtime();

        // Check and add nodes which do not exist in your membership list
        for (int i=0;i<sizeList;++i)
        {
            memcpy((char *)&newEntry, curr, sizeof(newEntry));
            curr = curr + sizeof(newEntry);

            bool exists = 0;
            for (auto &entry: node->memberList)
            {
                if (entry.id == newEntry.id and entry.port == newEntry.port)
                {
                    // Update heartbeat of existing nodes
                    if (newEntry.heartbeat > entry.heartbeat)
                    {
                        entry = newEntry;
                    }
                    exists = 1;
                }
            }

            if (!exists)
            {
                node->memberList.push_back(newEntry);
                Address address = idTOaddr(newEntry.id, newEntry.port);

                log->logNodeAdd(&(node->addr), &(address));
            }
        }
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // Update your own heartbeat
    for (auto &entry: memberNode->memberList)
    {
        Address address = idTOaddr(entry.id, entry.port);
        if (address == memberNode->addr)
        {
            entry.heartbeat = memberNode->heartbeat;
            entry.timestamp = par->getcurrtime();
        }
    }

    // Delete nodes after TREMOVE time
    for (auto it=memberNode->memberList.begin();it!=memberNode->memberList.end();)
    {
        if (par->getcurrtime() - it->timestamp > TREMOVE)
        {
            Address address = idTOaddr(it->id, it->port);
            log->logNodeRemove(&(memberNode->addr), &address);

            memberNode->memberList.erase(it);
        }
        else
            ++it;
    }

    // Send heartbeat to NGOSSIPS random nodes
    for (int i=0;i<NGOSSIPS;++i)
    {
        // Choose a node at random from the memberList
        MemberListEntry target = memberNode->memberList[rand()%((int)memberNode->memberList.size())];
        Address address = idTOaddr(target.id, target.port);

        size_t alive = 0;
        for (auto &entry: memberNode->memberList)
        {
            if (par->getcurrtime() - entry.timestamp <= TFAIL)
                alive++;
        }

        // Prepare a heartbeat message
        size_t msgsize = sizeof(MessageHdr) + sizeof(alive) + alive * sizeof(MemberListEntry);
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));

        msg->msgType = HBEAT;
        memcpy((char *)(msg+1), (char *)&alive, sizeof(alive));
        char *curr = (char *)(msg+1) + sizeof(alive);
        for (auto entry: memberNode->memberList)
        {
            if (par->getcurrtime() - entry.timestamp <= TFAIL)
            {
                memcpy(curr, &entry, sizeof(entry));
                curr = curr + sizeof(entry);
            }
        }

        // Send the heartbeat
        emulNet->ENsend(&(memberNode->addr), &(address), (char *)msg, msgsize);

        free(msg);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
