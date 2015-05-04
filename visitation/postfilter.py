#encoding: utf-8

__author__ = 'johann'

import unicodedata
import json

class PostFilter:

    def __init__(self):
        file = open('conf/termgroups.json')
        self.data = json.load(file)
        file.close()

        self.term_groups = self.data["termGroups"]
        self.reject = self.data["rejeitar"]

        self.strip_all_accents()

    def strip_all_accents(self):
        for group in self.term_groups:
            for term in group["aceitar"]:
                group["aceitar"][group["aceitar"].index(term)] = self.strip_accents(term).lower()

        for word in self.reject:
            self.reject[self.reject.index(word)] = self.strip_accents(word).lower()

    def find_raw_string(self, text):
        if '"' in text:
            range_slasher = [n for n in xrange(len(text)) if text.find('"', n) == n]
            return text[range_slasher[0] + 1 : range_slasher[1]]
        else: return None

    def detect_rejection(self, post):
        for text in self.reject:
            raw_string = self.find_raw_string(text)
            if raw_string:
                text = text.replace('"' + raw_string + '"', "")
                if raw_string in post:
                    return "REJECT"
                for word in text:
                    if word in post:
                        return "REJECT"
            else:
                text_splited = text.split(" ")
                for word in text_splited:
                    if word in post:
                        return "REJECT"

    def detect_group(self, post):
        groups = []

        for group in self.term_groups:
            for text in group["aceitar"]:
                raw_string = self.find_raw_string(text)
                if raw_string:
                    text_list = text.replace('"' + raw_string + '"', "").split(" ")
                    text_list.pop(text_list.index(""))

                    words_in_post = True

                    for word in text_list:
                        if word not in post:
                            words_in_post = False

                    if words_in_post and raw_string in post:
                        groups.append(group["key"])
                else:
                    text_list = text.split(" ")

                    words_in_post = True

                    for word in text_list:
                        if word not in post:
                            words_in_post = False
                    if words_in_post:
                        groups.append(group["key"])

        return groups

    def categorize_post(self, post):
        try:
            post = self.strip_accents(post.decode("utf-8")).lower()
        except UnicodeEncodeError:
            post = self.strip_accents(post).lower()

        if self.detect_rejection(post):
            return "REJECT"
        else:
            groups = self.detect_group(post)
            if groups:
                return list(set(self.detect_group(post)))

        return "UNCATEGORIZED"

    def strip_accents(self, string):
        return ''.join((c for c in unicodedata.normalize('NFD', unicode(string)) if unicodedata.category(c) != 'Mn'))

if __name__ == '__main__':
    f = PostFilter()
#
#    posts = ["estamos iniciando uma grande oferta de inauguração de eletrodomésticos",
#             "o luciano cartaxo fez isso e aquilo",
#             "o cartaxo, responsável pela prefeitura de joão pessoa",
#             "o senhor cartaxo é do pt",
#             "luciano lucélio cartaxo foi duas vezes o melhor...",
#             "neste sábado, nonato veio passar as férias em joão pessoa",
#             "nesta sexta, nonato veio para joão pessoa",
#             "os centros de REFERÊNCIA em educação infantil foram entregues",
#             "no hotel tambaú somos soberanas",
#             "a escola índio piragibe gostaria de apresentar uma grande baixaria",
#             "o trânsito em joão pessoa está a cada dia pior, o tempo só o deteriora",
#             "tem muito carro em joão pessoa, as ruas estão saturadíssimas",
#             "o @semobjp não responde mensagens",
#             "vendem-se bananas",
#             "cheguei de mala e cuia em joão pessoa, amo essa cidade, cujo prefeito é luciano cartaxo",
#             "estol rindo a bessa gent",
#             "tarloka?"]
#
#    for post in posts:
#        print post, f.categorize_post(post)
#
    print f.categorize_post("jhajha️")
