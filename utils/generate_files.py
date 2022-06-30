import random

PATH = 'files_'
WORDLIST = 'assets\\wordlist.txt'
WORDS_PER_FILE = 1000000
WORDS_PER_LINE = 100

def generate_file(filename):
    return open(filename, 'w+')

def generate_random_text(wordlist):
    text = ''
    for i in range(WORDS_PER_FILE):
        word = random.choice(wordlist).strip()
        text += word + ' '
        if (i % 100 == 0) and i > 0:
            text += "\n"
    return text

def generate_wordlist_array():
    wordlist = []
    with open(WORDLIST, 'r') as file:
        for line in file.readlines():
            wordlist.append(line.strip())
    return wordlist

def write_text(file, text):
    file.write(text)

wordlist = generate_wordlist_array()
for i in range(500):
    file = generate_file(PATH+f'uncompressed_{i+1}.txt')
    text = generate_random_text(wordlist)
    write_text(file, text)
    print(f"File {i+1} done!")