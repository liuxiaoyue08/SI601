##########################################
#### Regular Expression Tutorial #########
##########################################

import re
# Open the file (make sure its in the same directory as this file)
f = open('mbox-short.txt')
print f.read()
# Loop through each line of the file and count the lines
counterOfLines = 1

# process line by line
for line in f:
	line = line.rstrip() # strip white-spaces from the right
	if re.search('From:', line) : # search for From and print the line number/the actual line
		print "# On line: " + `counterOfLines` + " (Output) " + line 
	counterOfLines = counterOfLines + 1

f.close()

f = open('mbox-short.txt')
outfile = open('whoknowswho.txt', 'w')
outfile.write('Person\tKnows\n') # writes header

# now we slurp the whole thing in
all_lines = f.read()
f.close()

print all_lines
pairs = re.findall(r'To: (\S+).*?From: (\S+)', all_lines, re.DOTALL)
print pairs
for (known_person, person) in pairs:
	outfile.write(person + '\t' + known_person + '\n') # writes data

outfile.close()


