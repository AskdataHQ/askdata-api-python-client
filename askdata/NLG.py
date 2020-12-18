import operator
ops = {
    '<': operator.lt,
    '<=': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt
}

def get_conditional_phrases(conditions,phrase1,phrase2):
    bool_array = []
    for condition in conditions:
        for op in ops:
            if op in condition:
                splitted = condition.split(' '+op+' ')
                operator = ops[op]
                bool_array.append(operator(splitted[0],splitted[1]))
        #bool_array.append(eval(condition))
    if False in bool_array:
        return phrase2
    else:
        return phrase1


import random
def get_random_synonymous(synonyms):
    random.seed()
    nKeys = len(synonyms.keys())
    pickedItem = random.randint(0, nKeys-1)
    synPicked = synonyms[pickedItem]
    return synPicked

def words_to_digits(phrase):
    words = phrase.split(" ")
    num = []
    for word in words:
        if "." in word:
            word = word.replace(".","")
        if "," in word:
            word = word.replace(",","")
        if word.isdigit():
            num.append(int(word))
    return num


def add_random_synonymous_to_sentence(phrase,placeholder,synonyms):
    random.seed()
    nKeys = len(synonyms.keys())
    pickedItem = random.randint(0, nKeys-1)
    synPicked = synonyms[pickedItem]
    phrase = phrase.replace(placeholder,synPicked)
    return phrase