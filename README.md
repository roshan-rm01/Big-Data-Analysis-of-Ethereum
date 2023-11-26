# Big-Data-Analysis-of-Ethereum
An analysis of the Ethereum blockchain platform using big data tools like Apache PySpark.

## Introduction
The Ethereum dataset consists of 4 files, transactions.csv, blocks.csv, contracts.csv, and scams.csv from 2017 to 2019. The transactions.csv file contains a list of all Ethereum transactions, the blocks.csv file contains information about how a block is mined on the Ethereum network, the contracts.csv file contains information on the smart contracts that run on the Ethereum network, and the scams.csv contains a list of notable scams on the Ethereum network. Each file was stored in an Amazon S3 bucket and Apache PySpark was used to extract the files for analysis.

## Time Analysis
The number of monthly transactions that occurred between August 2015 and January 2019. The peak month of transactions happened in January 2018.
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/85a8cd0f-4ecd-4b94-a8c1-67716ecfd805)

The average value of a transaction (in Ether) between August 2018 and January 2019. The highest average transaction value was 1.43 Ether in May 2017.
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/247b80bd-c513-49a0-8afd-ae3cc9d6cbac)

## Most Popular Smart Contract
The top ten most popular smart contracts, where the most popular smart contract received 84155363.7 Ethers (Wei is converted to Ether by dividing by 10^618).
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/27f70d77-b432-4b0d-b46e-bab94da58f0d)

## Most Active Blockchain Miners
The top ten most active miners by mining block size in bytes. The most a miner has mined is 17453393724.00 bytes or 17.45 Gigabytes (converted by dividing by 10^9). 
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/9a63d9ec-4bec-4aee-ac39-3a717581bf65)

![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/65690101-3911-419e-9d71-4057a34e0d8b)

## Scam Analysis
The graph shows that over time, the amount Ether lost to scams had varying peaks of high losses during 2017 and 2018. The scam ID with the most lucrative scam is 2135 with 19110713823161336892288 Wei (19110.71 Ether).
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/ff3d9b8c-90f7-44f3-938c-aefd45855a7c)

## Gas Consumption
The gas price (in Ether) over time shows that in January 2018, the gas price was at its highest at 1.75 Ether.
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/6bcad518-270f-4e9d-9036-dfb43f90d145)

The gas used for contract transactions over time shows that in April 2018, 4.12E12 gas was used.
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/1796af49-733f-43c0-80a4-8d2139564e1f)

Average Gas Used: 263837.6006551491.
The gas used by the ten most popular smart contracts shows that all the contracts use more than the average gas by a significant margin.
![image](https://github.com/roshan-rm01/Big-Data-Analysis-of-Ethereum/assets/63264453/63dea97e-a533-4fe1-9ff2-d12b518de4d0)
