#!/bin/bash

echo ""
echo "Self configuring DLS/LFC Stress Test:"
echo "This script will run the ./run_test.py script, with the -w option, many times changing automatically"
echo "the parameters threadNum, nBlocks and nReplicas."
echo ""
echo "For more informations on the parameters read README file."
echo ""
echo "Please modify the parameters.py script so that the right DLS Client commands are used by the test suite."
echo ""
echo "WARNING: The script could take a lot of time to terminate."
echo -n "Proceed?(Yes/Y/No/N)":

read ANSWER
case $ANSWER in
        "Yes" | "Y" | "y" | "yes")      
		echo "Ready run" ;;
        "No" | "N" | "n" | "no")
                echo "Aborting"
                exit 0 ;;
        *)
                echo "Unrecognized value. No Assumed"
                exit 0 ;;
esac

echo "Run1: 5 threads, 4 blocks, 1 replica, poisson arrivals"
sed -i "s/threadNum = 10/threadNum = 5/" parameters.py;
./run_test.py -r > run1.out;
tar cvzf run1.tgz run1.out *.csv;
rm *.out *.csv

echo "Run2: 5 threads, 4 blocks, 1 replica, burst arrivals"
./run_test.py -r --distribution=burst > run2.out;
tar cvzf run2.tgz run2.out *.csv;
rm *.out *.csv

echo "Run3: 5 threads, 4 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run3.out;
tar cvzf run3.tgz run3.out *.csv;
rm *.out *.csv


sed -i "s/nBlocks = 4/nBlocks = 8/" parameters.py;

echo "Run3.1: 5 threads, 8 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run3.1.out;
tar cvzf run3.1.tgz run3.1.out *.csv;
rm *.out *.csv

echo "Run3.2: 5 threads, 8 blocks, 5 replica, pareto arrivals"
sed -i "s/nReplicas = 1/nReplicas = 5/" parameters.py;
./run_test.py -r --distribution=pareto > run3.2.out;
tar cvzf run3.2.tgz run3.2.out *.csv;
rm *.out *.csv
sed -i "s/nReplicas = 5/nReplicas = 1/" parameters.py;

sed -i "s/nBlocks = 8/nBlocks = 4/" parameters.py;

#------------------------------------------------------
sed -i "s/threadNum = 5/threadNum = 10/" parameters.py;

echo "Run4: 10 threads, 4 blocks, 1 replica, poisson arrivals"
./run_test.py -r > run4.out;
tar cvzf run4.tgz run4.out *.csv;
rm *.out *.csv

echo "Run5: 10 threads, 4 blocks, 1 replica, burst arrivals"
./run_test.py -r --distribution=burst > run5.out;
tar cvzf run5.tgz run5.out *.csv;
rm *.out *.csv

echo "Run6: 10 threads, 4 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run6.out;
tar cvzf run6.tgz run6.out *.csv;
rm *.out *.csv

sed -i "s/nBlocks = 4/nBlocks = 8/" parameters.py;
echo "Run6.1: 10 threads, 8 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run6.1.out;
tar cvzf run6.1.tgz run6.1.out *.csv;
rm *.out *.csv

echo "Run6.2: 10 threads, 8 blocks, 5 replica, pareto arrivals"
sed -i "s/nReplicas = 1/nReplicas = 5/" parameters.py;
./run_test.py -r --distribution=pareto > run6.2.out;
tar cvzf run6.2.tgz run6.2.out *.csv;
rm *.out *.csv
sed -i "s/nReplicas = 5/nReplicas = 1/" parameters.py;

sed -i "s/nBlocks = 8/nBlocks = 4/" parameters.py;

#------------------------------------------------------

sed -i "s/threadNum = 10/threadNum = 20/" parameters.py;

echo "Run7: 20 threads, 4 blocks, 1 replica, poisson arrivals"
./run_test.py -r > run7.out;
tar cvzf run7.tgz run7.out *.csv;
rm *.out *.csv

echo "Run8: 20 threads, 4 blocks, 1 replica, burst arrivals"
./run_test.py -r --distribution=burst > run8.out;
tar cvzf run8.tgz run8.out *.csv;
rm *.out *.csv

echo "Run9: 20 threads, 4 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run9.out;
tar cvzf run9.tgz run9.out *.csv;
rm *.out *.csv

sed -i "s/nBlocks = 4/nBlocks = 8/" parameters.py;

echo "Runi9.1: 20 threads, 8 blocks, 1 replica, pareto arrivals"
./run_test.py -r --distribution=pareto > run9.1.out;
tar cvzf run9.1.tgz run9.1.out *.csv;
rm *.out *.csv

echo "Run9.2: 20 threads, 8 blocks, 5 replica, pareto arrivals"
sed -i "s/nReplicas = 1/nReplicas = 5/" parameters.py;
./run_test.py -r --distribution=pareto > run9.2.out;
tar cvzf run9.2.tgz run9.2.out *.csv;
rm *.out *.csv
sed -i "s/nReplicas = 5/nReplicas = 1/" parameters.py;

sed -i "s/nBlocks = 8/nBlocks = 4/" parameters.py;

sed -i "s/threadNum = 20/threadNum = 10/" parameters.py
