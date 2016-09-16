#!/usr/bin/env python

# open the file containing data for each representative
IN = open('representativesparty.txt', 'rU')

years = set()
parties = set()
numinyearparty = {}

for line in IN: # loop through the file
    # split the contents of each line on the tab character
    [congnum, year, state, party, name] = line.split('\t')

    # keep a set of distinct years that occur in the dataset
    years.add(year)
    
    # keep a set of distinct parties that occur in the dataset
    parties.add(party)

    # create a tuple that holds the year and party, to be used as key
    pair = (year, party)

    # increment the counter for this year and party
    # e.g. the line "98	1983	ID	Republican	CRAIG  L"
    # would add count one more republican in the 98th congress (1983)
    if pair in numinyearparty:
        numinyearparty[pair] += 1
    else:
        numinyearparty[pair] = 1
        

# OK, time to output to the TSV file 
OUT = open('HouseRepPartiesByYear.txt', 'w')

# the first line will just have the headers
# the first column is the party and each subsequent column is the
# year that the congress started in
OUT.write("party")
for year in sorted(years):
    OUT.write('\t' + year)
OUT.write('\n')
# note that we have a tab separating the column headers
# and end with a newline

# now to output the actual data
# each row will correspond to a given party
# we'll sort them alphabetically just for kicks
for party in sorted(parties):
    # we print the party name
    OUT.write(party)

    # now we go through each year
    for year in sorted(years):
        # we again create the pair of year and party
        pair = (year, party)

        # if we did not record a count for
        # that party in that year, we set the output to zero
        if pair not in numinyearparty:
            count = 0
        else:
            count = numinyearparty[pair]

        # and then we output the count
        OUT.write('\t' + str(count))
    # after going through all the years for that party
    # we print a newline
    OUT.write('\n')

IN.close()
OUT.close()
