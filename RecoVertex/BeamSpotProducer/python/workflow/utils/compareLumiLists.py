#!/usr/bin/python

def compareLumiLists(logger, listA, listB, tolerance = 0, 
                     listAName = 'listA', listBName = 'listB'):
    '''
    Requires two lumi lists, listA and listB.
    Returns a list of lumi sections that are included in listA
    but not in listB and the equivalent list for A<-->B.
    A tolerance in per cent units can be passed. Default = 0.
    '''
    
    # this holds if there are no repeated LS in a single Run
    setA = set(listA)
    setB = set(listB)
    
    if len(setA) != len(listA) or len(setB) != len(listB):
        logger.error('\t\tRepeated lumi sections in {A} or {B}'\
                     ''.format(A = listAName,
                                    B = listBName))
    
    if setA == setB:
        return [], [] # all is well, return
    
    if len(listA) < len(listB) * (1 - 0.01 * tolerance):
        logger.error('\t\tThe number of lumi sections is different: '\
                     '{A}({LENA})!=(LENB){B}'                        \
                     ''.format(A    = lisAName ,
                               LENA = len(setA),
                               LENB = len(setB),
                               B    = listBName))

    badB = setA - setB        
    badA = setB - setA        
    
    for lumi in badB:
        logger.error('\t\tLumi {LUMI} is in {A} but not in {B}'\
                     ''.format(LUMI = lumi     , 
                               A    = listAName,
                               B    = listBName))        

    for lumi in badA:
        logger.error('\t\tLumi {LUMI} is in {B} but not in {A}'\
                     ''.format(LUMI = lumi     , 
                               B    = listBName,
                               A    = listAName))        

    return badA, badB

if __name__ == '__main__':
    print 'Ehm, yet to be implemented, sorry'
    # needs a logger to run properly
