##########################################
#### String Tutorial #####################
########################################## 

stringHello = "hello from Ann Arbor!"

#capitalize(...)
#    S.capitalize() -> string
#    
#    Return a copy of the string S with only its first character
#    capitalized.
print stringHello.capitalize()
# OUTPUT: Hello from ann arbor!

#swapcase(...)
#    S.swapcase() -> string
#    
#    Return a copy of the string S with uppercase characters
#    converted to lowercase and vice versa.
stringHello.swapcase()
# OUTPUT: HELLO FROM aNN aRBOR!

#title(...)
#    S.title() -> string
#    
#    Return a titlecased version of S, i.e. words start with uppercase
#    characters, all remaining cased characters have lowercase.
stringHello.title()
# OUTPUT: Hello From Ann Arbor!

#lower(...)
#    S.lower() -> string
#    
#    Return a copy of the string S converted to lowercase.
#upper(...)
#    S.upper() -> string
#    
#    Return a copy of the string S converted to uppercase.
print stringHello.lower()
#OUTPUT: hello from ann arbor!
print stringHello.upper()
#OUTPUT: HELLO FROM ANN ARBOR!

#center(...)
#    S.center(width[, fillchar]) -> string
#    
#    Return S centered in a string of length width. Padding is
#    done using the specified fill character (default is a space)
print "ciao".center(1)
print "ciao".center(2)
print "ciao".center(4)
print "ciao".center(5)
print "ciao".center(6)
print "ciao".center(10, "!")
# OUTPUT: !!!ciao!!!

#count(...)
#    S.count(sub[, start[, end]]) -> int
#    
#    Return the number of non-overlapping occurrences of substring sub in
#    string S[start:end].  Optional arguments start and end are interpreted
#    as in slice notation.
print stringHello.count("hello")
print stringHello.count("o")
print stringHello.count("o", 6)
#OUTPUT: 2
print stringHello[6:]
#OUTPUT: from Ann Arbor!
print stringHello.count("o", 6, 3) # not what we are looking for!
#OUTPUT: 0
print stringHello.count("o", 6, len(stringHello)-3)
#OUTPUT: 1
print stringHello[6:len(stringHello)-3] 
#OUTPUT: from Ann Arb

#### endswith(suffix,start=0,end=sys.maxint) or startswith #####################
# returns true or false if ends/starts with suffix
print stringHello.endswith("Arbor!")
#OUTPUT: TRUE
print stringHello.endswith("Arbor") # No "!"
#OUTPUT: FALSE
print stringHello.startswith("hello")
#OUTPUT: TRUE

#expandtabs(...)
#    S.expandtabs([tabsize]) -> string
#    
#    Return a copy of S where all tab characters are expanded using spaces.
#    If tabsize is not given, a tab size of 8 characters is assumed.
somethingElse = "%s\t%s\t%s\t\t%s?" % ("this","is","tabbed","see?")
print somethingElse
#OUTPUT: this    is      tabbed          see??
print somethingElse.expandtabs(15)
#OUTPUT: this           is             tabbed                        see??

#find(...)
#    S.find(sub [,start [,end]]) -> int
#    
#    Return the lowest index in S where substring sub is found,
#    such that sub is contained within S[start:end].  Optional
#    arguments start and end are interpreted as in slice notation.
#    
#    Return -1 on failure.
#    
#rfind(...)
#    S.rfind(sub [,start [,end]]) -> int
#    
#    Return the highest index in S where substring sub is found,
#    such that sub is contained within S[start:end].  Optional
#    arguments start and end are interpreted as in slice notation.
#    
#    Return -1 on failure.
print len(stringHello)
#OUTPUT 21
print stringHello.find("o")
#OUTPUT: 4
print stringHello.rfind("o")
#OUTPUT: 18

#index(...)
#    S.index(sub [,start [,end]]) -> int
#    
#    Like S.find() but raise ValueError when the substring is not found.
print stringHello.index("z")
#OUTPUT: Traceback (most recent call last):
# File "<stdin>", line 1, in <module>
# ValueError: substring not found

#### isalnum, isalpha, isdigit, islower, isspace, istitle, isupper #####################
# returns true if.. well.. true. False if not.
print stringHello.isdigit()
#OUTPUT: False
print stringHello.isalpha()
#OUTPUT: False
print stringHello.isalnum()
#OUTPUT: False 

#ljust(...)
#    S.ljust(width[, fillchar]) -> string
#    
#    Return S left-justified in a string of length width. Padding is
#    done using the specified fill character (default is a space).
#rjust(...)
#    S.rjust(width[, fillchar]) -> string
#    
#    Return S right-justified in a string of length width. Padding is
#    done using the specified fill character (default is a space)
print stringHello.ljust(len(stringHello)+2,"x")
#OUTPUT: hello from Ann Arbor!xx
print stringHello.rjust(len(stringHello)+2,"x")
#OUTPUT: xxhello from Ann Arbor!

#### lstrip(x=string.whitespace) or rstrip or strip #####################
# remove x (default, whitespaces) from l(eft) or r(ight) or both sides
somethingElse = "this is a sentence    " 
print somethingElse.rstrip()
# OUTPUT: this is a sentence
print stringHello.rstrip("!")
# OUTPUT: hello from Ann Arbor
print stringHello.lstrip("h")
# OUTPUT: ello from Ann Arbor!

#replace(...)
#    S.replace(old, new[, count]) -> string
#    
#    Return a copy of string S with all occurrences of substring
#    old replaced by new.  If the optional argument count is
#    given, only the first count occurrences are replaced.
stringHello.replace("e", "a")
#OUTPUT: hallo from Ann Arbor!
stringHello.replace("o", "0", 2)
#OUTPUT: hell0 fr0m Ann Arbor!

