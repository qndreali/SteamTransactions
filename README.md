To simulate Case 1:
    1. Turn off Node 1 in sidebar.
    2. Choose from Insert, Update, Delete
    3. Perform transaction. Should replicate to Node 2 or Node 3 (depending on year partition). Unable to replicate to Node 1.
    4. Turn on Node 1 again. Should automatically sync chages.

To simulate Case 2.1:
    0. Turn on all nodes
    1. Turn off Node 2 in side bar 
    2. Transaction on Node 1. ( date < 2010)
    3. Turn on Node 2 again. Should automatically recover and replicate any changes that were missed during the downtime.
To simulate Case 2.2:
    0. Turn on all nodes
    1. Turn off Node 3 in side bar 
    2. Transaction on Node 1. ( date > 2010)
    3. Turn on Node 3 again. Should automatically recover and replicate any changes that were missed during the downtime.
    
To simulate Case 3:
    1. Turn off Node 1 in sidebar.
    2. Turn on "Simulate Node 1 replication failure" from sidebar
    3. Choose from Insert, Update, Delete
    4. Perform transaction. Should replicate to Node 2 or Node 3 (depending on year partition). Unable to replicate to Node 1.
    5. Turn on Node 1 again. Should have "replicattion failure."
    6. Turn off "Simulate Node 1 replication failure" from sidebarShould automatically sync chages.

To simulate Case 4.1:
    1. Turn off Node 2 and 3 in sidebar.
    2. Turn on "Simulate Node 2 or 3 replication failure" from sidebar
    3. Choose from Insert, Update, Delete
    4. Perform transaction (date >= 2010). Should replicate to Node 1. Unable to replicate to Node 2 or 3.
    5. Turn on Node 3. Should have "replicattion failure."
    6. Turn off "Simulate Node 2 or 3 replication failure" from sidebar. Should automatically sync chages.