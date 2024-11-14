my_dict = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
keys_to_remove = ['b', 'c']



for key in keys_to_remove:
    my_dict.pop(key, None)  # Использование pop с аргументом по умолчанию

print(my_dict)  # Вывод: {'a': 1, 'd': 4}