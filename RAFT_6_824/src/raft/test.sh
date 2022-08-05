python dstest.py -p 5 -n 50 -r TestBasic3A
# python dstest.py -p 5 -n 10 -r TestSpeed3A
python dstest.py -p 5 -n 50 -r TestConcurrent3A
python dstest.py -p 5 -n 50 -r TestUnreliable3A
python dstest.py -p 5 -n 50 -r TestUnreliableOneKey3A
python dstest.py -p 5 -n 50 -r TestOnePartition3A
python dstest.py -p 5 -n 50 -r TestManyPartitionsOneClient3A
python dstest.py -p 5 -n 50 -r TestManyPartitionsManyClients3A
# python dstest.py -p 5 -n 10 -r TestPersistOneClient3A
# python dstest.py -p 5 -n 10 -r TestPersistConcurrent3A
# python dstest.py -p 5 -n 10 -r TestPersistConcurrentUnreliable3A
# python dstest.py -p 5 -n 10 -r TestPersistPartition3A
# python dstest.py -p 5 -n 10 -r TestPersistPartitionUnreliable3A
# python dstest.py -p 5 -n 10 -r TestPersistPartitionUnreliableLinearizable3A