/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *              Definition of MP1Node class functions.
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
    this->payload.clear();
    this->checkPos = this->memberNode->memberList.end();
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *              This function is called by a node to receive messages currently waiting for it
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
 *              All initializations routines for a member.
 *              Called by the application layer.
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
    memberNode->pingCounter = TPING;
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
        checkPos = memberNode->memberList.end();

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
 *              Check your messages in queue and perform membership protocol duties
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
    memcpy(&(address.addr[0]), (char *)&id, sizeof(int));
    memcpy(&(address.addr[4]), (char *)&port, sizeof(short));
    return address;
}

// Function to convert an Addrees to its id and port
void addrToid(Address addr, int &id, short &port)
{
    id = *(int*)(addr.addr);
    port = *(short*)(addr.addr+4);
}

/**
 * FUNCTION NAME: refreshPayload
 *
 * DESCRIPTION: Remove stale entries from payload
 */
void MP1Node::refreshPayload()
{
    for (auto it=payload.begin();it!=payload.end();)
    {
        if (par->getcurrtime() - it->timestamp > TREMOVE)
            payload.erase(it);
        else
            it++;
    }
}

/**
 * FUNCTION NAME: findPayload
 *
 * DESCRIPTION: Finds a payload entry
 */
vector<PayloadMember>::iterator MP1Node::findPayload(PayloadMember pay)
{
    for (auto it=payload.begin();it!=payload.end();++it)
    {
        if (it->id == pay.id and it->port == pay.port)
        {
            if (pay.heartbeat > it->heartbeat)
            {
                it->heartbeat = pay.heartbeat;
                it->timestamp = par->getcurrtime();
                it->status = pay.status;
            }
            return it;
        }
    }
    return payload.end();
}

/**
 * FUNCTION NAME: findMember
 *
 * DESCRIPTION: Finds a memberlist entry
 */
vector<MemberListEntry>::iterator MP1Node::findMember(MemberListEntry mem)
{
    for (auto it=memberNode->memberList.begin();it!=memberNode->memberList.end();++it)
    {
        if (it->id == mem.id and it->port == mem.port)
        {
            if (mem.heartbeat > it->heartbeat)
            {
                it->heartbeat = mem.heartbeat;
                it->timestamp = par->getcurrtime();
            }
            return it;
        }
    }
    return memberNode->memberList.end();
}

/**
 * FUNCTION NAME: updateLists
 *
 * DESCRIPTION: Add entries recieved as payload from the message
 */
void MP1Node::updateLists(char *curr)
{
    // Find size of recieved payload
    size_t sizeList;
    memcpy((char *)&sizeList, curr, sizeof(sizeList));
    curr += sizeof(sizeList);

    int nocheckPos = 0, id;
    short port;
    if (checkPos == memberNode->memberList.end())
        nocheckPos = 1;
    else
    {
        id = checkPos->id;
        port = checkPos->port;
    }

    for (int i=0;i<sizeList;++i)
    {
        // Insert to payload if doesn't exist already
        PayloadMember pay;
        memcpy((char *)&pay, curr, sizeof(pay));
        curr += sizeof(pay);
        if (findPayload(pay) == payload.end())
            payload.push_back(pay);

        // Insert to/ Delete from memberList
        MemberListEntry mem(pay.id, pay.port, pay.heartbeat, par->getcurrtime());
        Address address = idTOaddr(pay.id, pay.port);
        if (pay.status == true and findMember(mem) == memberNode->memberList.end())
        {
            memberNode->memberList.push_back(mem);
            log->logNodeAdd(&(memberNode->addr), &address);
        }
        else if (pay.status == false and findMember(mem) != memberNode->memberList.end())
        {
            memberNode->memberList.erase(findMember(mem));
            log->logNodeRemove(&(memberNode->addr), &address);
        }
    }

    // Put checkPos at end if it was at the end before
    if (nocheckPos)
        checkPos = memberNode->memberList.end();
    else
        checkPos = findMember(MemberListEntry (id, port));
}

/**
 * FUNCTION NAME: pushPayload
 *
 * DESCRIPTION: Push your payload onto the message
 */
void MP1Node::pushPayload(char *msg)
{
    size_t sizeList = payload.size();
    memcpy(msg, (char *)&sizeList, sizeof(sizeList));
    msg += sizeof(sizeList);

    for (int i=0;i<sizeList;++i)
    {
        memcpy(msg, (char *)&(payload[i]), sizeof(PayloadMember));
        msg += sizeof(PayloadMember);
    }
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    Member *node = (Member *)env;
    enum MsgTypes type = ((MessageHdr *)data)->msgType;
    char *curr = (char *)((MessageHdr *)data+1);

    if (type == JOINREQ)
    {
        int nocheckPos = 0, idO;
        short portO;
        if (checkPos == memberNode->memberList.end())
            nocheckPos = 1;
        else
        {
            idO = checkPos->id;
            portO = checkPos->port;
        }

        // Add new node in coordinator's membership list
        Address address;
        long heartbeat;
        memcpy((char *)&address, (char *)((MessageHdr *)data+1), sizeof(Address));
        memcpy((char *)&heartbeat, (char *)((MessageHdr *)data+1) + 1 + sizeof(Address), sizeof(long));

        int id = *(int*)(address.addr);
        int port = *(short*)(address.addr+4);

        MemberListEntry newMember(id, port, heartbeat, par->getcurrtime());
        node->memberList.push_back(newMember);

        payload.push_back(PayloadMember (newMember, true));

        // Create a log for added node
        log->logNodeAdd(&(node->addr), &(address));

        // Create a JOINREP message for new node
        size_t sizeList = node->memberList.size();
        size_t msgsize = sizeof(MessageHdr) + sizeof(sizeList) + sizeList * sizeof(MemberListEntry);
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        msg->msgType = JOINREP;
        memcpy((char *)(msg+1), (char *)&(sizeList), sizeof(sizeList));
        char *top = (char *)(msg+1) + sizeof(sizeList);
        for (auto entry: node->memberList)
        {
            memcpy(top, (char *)&(entry), sizeof(entry));
            top = top + sizeof(entry);
        }

        // Put checkPos at end if it was at the end before
        if (nocheckPos)
            checkPos = memberNode->memberList.end();
        else
            checkPos = findMember(MemberListEntry (idO, portO));

        // Send JOINREP to newly added node
        emulNet->ENsend(&(node->addr), &address, (char *)msg, msgsize);

        free(msg);
    }
    else if (type == JOINREP)
    {
        node->inGroup = 1;

        // Add all elements to your membership list
        size_t sizeList;
        char *curr = (char *)((MessageHdr *)data+1);

        memcpy((char *)&sizeList, curr, sizeof(sizeList));
        curr = curr + sizeof(sizeList);

        MemberListEntry newEntry;
        for (int i=0;i<sizeList;++i)
        {
            memcpy((char *)&newEntry, curr, sizeof(newEntry));
            curr = curr + sizeof(newEntry);
            newEntry.timestamp = par->getcurrtime();
            node->memberList.push_back(newEntry);

            PayloadMember pay(newEntry, true);
            payload.push_back(pay);

            Address address = idTOaddr(newEntry.id, newEntry.port);

            log->logNodeAdd(&(node->addr), &(address));
        }

        checkPos = memberNode->memberList.end();
    }
    else if (type == PING)
    {
        // Remove the expired elements
        refreshPayload();

        // Prepare the ACK message to send
        size_t sizeList = payload.size();
        int msgsize = sizeof(MessageHdr) + 2 * sizeof(Address)
                        + sizeof(sizeList) + sizeList * sizeof(PayloadMember);
        MessageHdr *msgHead = (MessageHdr *) malloc(msgsize * sizeof(char));
        msgHead->msgType = ACK;

        // Process the recieved message
        Address pinger;
        memcpy((char *)&pinger, curr, sizeof(pinger));
        curr += sizeof(pinger);

        // Add addresses and payload on ACK
        memcpy((char *)(msgHead+1), (char *)&(node->addr), sizeof(Address));
        memcpy((char *)(msgHead+1) + sizeof(Address), (char *)&pinger, sizeof(Address));

        // Push your payload data
        pushPayload((char *)(msgHead+1) + 2 * sizeof(Address));
        
        // Update your payload and membership lists
        updateLists(curr);

        // Send ACK message to the pinger
        emulNet->ENsend(&(node->addr), &pinger, (char *)msgHead, msgsize);

        free(msgHead);
    }
    else if (type == ACK)
    {
        // Parse the recieved information
        Address acker, dest;
        memcpy((char *)&acker, curr, sizeof(acker));
        curr += sizeof(acker);
        memcpy((char *)&dest, curr, sizeof(dest));
        curr += sizeof(dest);

        // Update your payload and membership lists
        updateLists(curr);

        // If you aren't the required destination, forward the message
        if (!(dest == node->addr))
        {
            emulNet->ENsend(&(node->addr), &(dest), data, size);
            return 1;
        }

        // Check if the recieved ACK is from required node
        if (checkPos != node->memberList.end() and acker == idTOaddr(checkPos->id, checkPos->port))
        {
            checkPos->timestamp = par->getcurrtime();
        }
    }
    else if (type == PING_REQ)
    {
        // Remove the expired elements
        refreshPayload();

        // Read pinger(source) and destination addresses
        Address pinger, dest;
        memcpy((char *)&pinger, curr, sizeof(Address));
        curr += sizeof(Address);
        memcpy((char *)&dest, curr, sizeof(Address));
        curr += sizeof(Address);

        // Prepare the PING message to send
        size_t sizeList = payload.size();
        int msgsize = sizeof(MessageHdr) + sizeof(Address)
                        + sizeof(sizeList) + sizeList * sizeof(PayloadMember);
        MessageHdr *msgHead = (MessageHdr *) malloc(msgsize * sizeof(char));
        msgHead->msgType = PING;

        // Add addresses and payload on PING
        memcpy((char *)(msgHead+1), (char *)&pinger, sizeof(Address));

        // Push your payload data
        pushPayload((char *)(msgHead+1) + sizeof(Address));

        // Update your payload and membership lists
        updateLists(curr);

        // Send PING message to the dest
        emulNet->ENsend(&(node->addr), &dest, (char *)msgHead, msgsize);

        free(msgHead);
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *              the nodes
 *              Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // Update your own heartbeat
    MemberListEntry mem;
    addrToid(memberNode->addr, mem.id, mem.port);
    auto it = findMember(mem);

    if (memberNode->heartbeat % TPING == 0 and memberNode->memberList.size() > 1)
    {
        if (checkPos == memberNode->memberList.end() or par->getcurrtime() - checkPos->timestamp < TPING)
        {
            // Find a node other than itself to ping
            checkPos = memberNode->memberList.begin() + rand()%memberNode->memberList.size();
            while (idTOaddr(checkPos->id, checkPos->port) == memberNode->addr)
            {
                checkPos = memberNode->memberList.begin() + rand()%memberNode->memberList.size();
            }
            checkPos->timestamp = par->getcurrtime();

            // Remove the expired elements
            refreshPayload();

            // Prepare the PING message to send
            size_t sizeList = payload.size();
            int msgsize = sizeof(MessageHdr) + sizeof(Address)
                            + sizeof(sizeList) + sizeList * sizeof(PayloadMember);
            MessageHdr *msgHead = (MessageHdr *) malloc(msgsize * sizeof(char));
            msgHead->msgType = PING;

            // Add your address on the PING
            memcpy((char *)(msgHead+1), (char *)&(memberNode->addr), sizeof(Address));

            // Push your payload data
            pushPayload((char *)(msgHead+1) + sizeof(Address));

            // Send PING message to the dest
            Address target = idTOaddr(checkPos->id, checkPos->port);
            emulNet->ENsend(&(memberNode->addr), &target, (char *)msgHead, msgsize);
        }
        else if (par->getcurrtime() - checkPos->timestamp >= TPING and
                    par->getcurrtime() - checkPos->timestamp < 2*TPING)
        {
            // Remove the expired elements
            refreshPayload();

            int maxpingers = max(0, (min(FORWARD_PINGERS, (int)memberNode->memberList.size()-2)));
            vector<MemberListEntry> Fpingers(maxpingers);
            for (int i=0;i<maxpingers;++i)
            {
                Fpingers[i] = memberNode->memberList[rand() % memberNode->memberList.size()];
                while ((Fpingers[i].id == checkPos->id and Fpingers[i].port == checkPos->port) or
                        idTOaddr(Fpingers[i].id, Fpingers[i].port) == memberNode->addr)
                    Fpingers[i] = memberNode->memberList[rand()%memberNode->memberList.size()];
            }

            // Prepare the PING_REQ message to send
            size_t sizeList = payload.size();
            int msgsize = sizeof(MessageHdr) + 2 * sizeof(Address)
                            + sizeof(sizeList) + sizeList * sizeof(PayloadMember);
            MessageHdr *msgHead = (MessageHdr *) malloc(msgsize * sizeof(char));
            msgHead->msgType = PING_REQ;

            // Add addresses on the PING_REQ
            memcpy((char *)(msgHead+1), (char *)&(memberNode->addr), sizeof(Address));
            Address target = idTOaddr(checkPos->id, checkPos->port);
            memcpy((char *)(msgHead+1) + sizeof(Address), (char *)&target, sizeof(Address));

            // Push your payload data
            pushPayload((char *)(msgHead+1) + 2 * sizeof(Address));

            // Send PING_REQ message to the forwarders
            for (int i=0;i<maxpingers;++i)
            {
                Address forwarder = idTOaddr(Fpingers[i].id, Fpingers[i].port);
                emulNet->ENsend(&(memberNode->addr), &forwarder, (char *)msgHead, msgsize);
            }
        }
        else
        {
            PayloadMember pay(*checkPos, false);
            payload.push_back(pay);

            Address address = idTOaddr(checkPos->id, checkPos->port);
            log->logNodeRemove(&(memberNode->addr), &address);

            memberNode->memberList.erase(checkPos);
            checkPos = memberNode->memberList.end();
        }
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
