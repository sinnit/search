# -*- coding: UTF-8 -*-
__author__ = 'Chernov.Artem'

import os
import re
from glob import glob
import logging
from pprint import pprint

class PastuhLib(object):

    DIR_FILES = 'files'
    NAME_TMP_FILES = 'tmp.txt'
    NAME_LEMM_FILES = 'lemma.txt'

    def __init__(self):
        # получение списка файлов
        files_name = self.get_files_name(self.DIR_FILES)
        self.run(files_name)
        # считаем для каждой записи произведение повторение и общей частотности
        self.count_repeats()
        self.count_amount()

    def run(self, files_name):
        for file_name in files_name:
            # запись запрсов из файла в отдельный файл
            self.query_to_file(file_name)
            # запуск стемирования запросов в файле
            self.run_mystem()
            # анализ лематизированного файла и поиск в нем строк попадающих под фильтр
            self.find_queries(file_name)

    def count_amount(self):
        total_data = {}
        files_name = self.get_files_name(os.path.join(self.DIR_FILES, 'result'))
        for file_name in files_name:
            query = self.get_file_basename(file_name)
            # получаем ключевой запрос
            query = query.group(1).lower()
            total_data[query] = []

            file = open(file_name, encoding='utf-8')
            for num, line in enumerate(file):
                # удаляем символ переноса строки в конце
                chunk_line = re.split(';', line)[:-1]
                if len(chunk_line) != 6:
                    logging.warning('В строка запроса {0} не соответсвует формату или была уже обработана'.format(num))
                    continue
                total_data[query].append(chunk_line)

        # проходим список файлов
        for key in total_data:
            # проходим список строк в файле
            for data in total_data[key]:
                # удалили из запроса ключевые слова
                data[0] = re.sub(key, '', data[0], flags=re.I).strip()

        result = {}
        # проходим список файлов
        for key in total_data:
            # проходим список строк в файле
            for data in total_data[key]:
                try:
                    sum = int(data[5])
                except ValueError as e:
                    continue

                query = data[0]
                if not query:
                    continue
                if result.get(query, False):
                    result[query][0] += sum
                    result[query][1].append([key, sum])
                else:
                    result[query] = [sum, [[key, sum]]]

        sort_result = list(result.items())
        # сохроняем результат в файл
        file = open('{0}\{1}\{2}'.format(self.DIR_FILES, 'tmp', 'total.txt'), 'w', encoding='utf-8')
        # сортиуем список, первый элемент в списке это общая сумма
        sort_result.sort(key=lambda item: item[1][0], reverse=True)
        for item in sort_result:
            to_save = [item[0], str(item[1][0])]
            # сортируем количество ключевых слов в каждом файле
            item[1][1].sort(key=lambda count: count[1], reverse=True)
            to_save.extend([str(word) for words in item[1][1] for word in words])
            to_save.append("\n")
            file.write(';'.join(to_save))
        file.close()

    def count_repeats(self):
        """
        считаем для каждой записи произведение повторение и общей частотности
        """
        files_name = self.get_files_name(os.path.join(self.DIR_FILES, 'result'))
        # проходим все файлы которые попали в результируюшйи набор
        for file_name in files_name:
            file = open(file_name, encoding='utf-8')
            # локальные переменые для храниея запросов
            uniq_query, rest = {}, []

            for num, line in enumerate(file):
                # удаляем символ переноса строки в конце
                chunk_line = re.split(';', line)[:-1]
                # если длина не соответствует, игнорируем строку
                if len(chunk_line) != 5:
                    #logging.warning('В строка запроса {0} не соответсвует формату или была уже обработана'.format(num))
                    continue
                # получаем запрос
                query = chunk_line[0]
                # сохроняем оставшуюся часть, откидываем последний элемент массива, он пустой
                rest.append(chunk_line)
                if uniq_query.get(query, False):
                    uniq_query[query] += 1
                else:
                    uniq_query[query] = 1
            file.close()

            if not len(rest):
                continue

            # сохроняем результат в файл
            result = self.leave_best_value(rest, uniq_query)
            file = open(file_name, 'w', encoding='utf-8')
            for line in result:
                line[-1] = str(line[-1])
                # добавление разделителя
                line.append("\n")
                file.write(';'.join(line))
            file.close()

    def leave_best_value(self, rest, uniq_query):
        # счиатем произведение для каждого значения
        for data in rest:
            if not uniq_query.get(data[0]):
                raise Exception('Не найден запрос в результируещем словаре')

            WS = data[1]
            if WS == '':
                WS = 0

            data.append(int(uniq_query.get(data[0])) * int(WS))

        # оставляем только максимальные значения
        max_value = {}
        for value in rest:
            if max_value.get(value[0], False):
                if value[-1] > max_value[value[0]][-1]:
                    max_value[value[0]] = value
            else:
                max_value[value[0]] = value
        values = list(max_value.values())
        # фильтруем данные по убыванию
        values.sort(key=lambda val: val[-1], reverse=True)
        return values

    def get_file_basename(self, file_name):
        """
        получение из имени файла, ключевой запрос
        :return Match object
        """
        name = os.path.basename(file_name)
        # поиск запроса в имени файла, используеться для анализа текста
        query = re.match('\[([^\]]*)\]\.csv', name, re.I | re.U)
        if not query:
            raise Exception('Не соответствует  паттерн для имени файле {0}'.format(name))
        return query

    def find_queries(self, file_name):
        """
        анализ лематизированного файла и поиск в нем строк попадающих под фильтр
        """
        name = os.path.basename(file_name)
        # поиск запроса в имени файла, используеться для анализа текста
        query = self.get_file_basename(file_name)

        # если слов несколько, то получаем последнее
        last_word = query.group(1).split(' ')[-1]

        file = open('{0}\{1}\{2}'.format(self.DIR_FILES, 'tmp', self.NAME_LEMM_FILES), encoding='utf-8')

        to_saving = []
        for num, line in enumerate(file):
            line = line.strip()
            # метод может меняться в зависимости от того как данные следует расчитать
            text = self.check_result(line, last_word)
            if not text:
                continue
            # вставляем текст в первую позицию возвращаемого рещультата
            self.rest[num].insert(0, text)
            to_saving.append(self.rest[num])

        file.close()

        # сохроняем результат в файл
        file = open('{0}\{1}\{2}'.format(self.DIR_FILES, 'result', name), 'w', encoding='utf-8')
        for line in to_saving:
            file.write(';'.join(line))
        file.close()

    def check_result(self, line, word):
        """
        поиск вхождения слов и следующего за ним предлога
        :param line: строка в которой идет поиск
        :param word: ключевое слово которое нужно найти
        :return: False или запрос из строки
        """
        pattern = '('+word+'{([^}]*)} \w+{([^}]*).*)'
        result = re.search(pattern, line, re.I)
        if not result:
            return False
        # проверяем что в тексте есть падеж и за ним следует предлог
        if not re.search('=им', result.group(2)) or not re.search('=PR', result.group(3)):
            return False
        # удаляме из текста все лишнее (все что находиться в фигурных скобках)
        return re.sub('{[^}]*}', '', result.group(1))

    def check_result_2(self, line, word):
        """
        поиск вхождения слов перед которыми идут прилагательные
        :param line: строка в которой идет поиск
        :param word: ключевое слово которое нужно найти
        :return: False или запрос из строки
        """
        pattern = '\w+{([^}]*)} '+word
        result = re.search(pattern, line, re.I)
        if not result:
            return False
        # проверяем что в тексте перед словом идет прилагательное
        if not re.search('=A', result.group(1)):
            return False
        # удаляме из текста все лишнее (все что находиться в фигурных скобках)
        return re.sub('{[^}]*}', '', result.group(0))

    def query_to_file(self, file_name):
        """
        запись запрсов из файла в отдельный файл
        :param file_name: string имя файла
        :return:
        """
        if not os.path.isfile(file_name):
            raise Exception("Файл {0} не найден".format(file_name))

        file = open(file_name, encoding='cp1251')
        # проходим весь файл, исключая заголовок и выбираем только строку запроса
        query_to_save = [re.split(';', line) for num, line in enumerate(file) if num]
        file.close()
        if not len(query_to_save):
            raise Exception('Ни одной строки не прочитано в файле {0}'.format(file_name))

        # сохроняем часть данных из входного файла
        self.rest = [chank[1:] for chank in query_to_save]
        # сохроянем запросы для лематизации
        query_to_save = [chank[0] for chank in query_to_save]
        # сохронение запросов во временный файл, для mystema
        file = open('{0}\{1}\{2}'.format(self.DIR_FILES, 'tmp', self.NAME_TMP_FILES), 'w', encoding='utf-8')
        file.write('\n'.join(query_to_save))
        file.close()

    def get_files_name(self, path_to_files):
        """
        получение списка файлов
        """
        files_name = glob('{0}/*.csv'.format(path_to_files))
        if not len(files_name):
            raise Exception("Файлы не найдены")
        return files_name

    def run_mystem(self):
        """
        запуск стемирования запросов в файле
        """
        run_comman = 'mystem\mystem -csi {0}\{1}\{2} {0}\{1}\{3}'.format(self.DIR_FILES, 'tmp', self.NAME_TMP_FILES, self.NAME_LEMM_FILES)
        result = os.system(run_comman)

if __name__ == '__main__':
    PastuhLib()
