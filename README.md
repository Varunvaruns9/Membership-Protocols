# Membership Protocols

This is an implementation of All to All, Gossip style and SWIM Membership Protocols. They are used for peer to peer communication in distributed systems.

These protocols always satisfy:
i) Completeness all the time: every non-faulty process must detect every node join, failure, and leave.
ii) Accuracy of failure detection when there are no message losses and message delays are small.
When there are message losses, completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.

## Testing

Compile the code using the makefiles provided in each folder. Change the testcases as required to test on different use cases.

An emulated network layer (EmulNet) is used for testing the working of the protocols.

Please refer to the pdf documents in each folder for more info.


Credits to Coursera and University of Illinois at Urbana-Champaign for the Network Emulation code and other scripts as a part of their MP1 Assignment.
