import tkinter as tk
import os
import io
from tkinter import *
import tkinter.filedialog as fd

filepath1 = r""
filepath2 = r""
files_to_hash = {"filepath1": filepath1, "filepath2": filepath2}


class App1(tk.Tk):

    def __init__(self):
        super().__init__()
        self.btn_file1 = tk.Button(self, text="Выбрать файл 1", command=self.choose_file1)
        self.btn_file2 = tk.Button(self, text="Выбрать файл 2", command=self.choose_file2)
        self.btn_start = tk.Button(
            self, text="Начать сравнение", width=50, height=2, command=self.start
        )
        self.btn_file1.pack(padx=100, pady=10)
        self.btn_file2.pack(padx=100, pady=10)
        self.btn_start.pack(padx=100, pady=10)

    def choose_file1(self):
        self.btn_file1.config(bg="white")
        filetypes = (
            ("Любой", "*"),
            ("Таблица", "*.csv *.xls *.xlsx"),
            ("Изображение", "*.jpg *.gif *.png"),
            ("Текстовый файл", "*.txt"),
        )
        filepath1 = fd.askopenfilename(
            title="Открыть файл", initialdir="/", filetypes=filetypes
        )
        if filepath1:
            files_to_hash.update({"filepath1": filepath1})
            print("Выбран файл: ", filepath1)
            self.btn_file1.config(bg="green")
            return filepath1

    def choose_file2(self):
        self.btn_file2.config(bg="white")
        filetypes = (
            ("Любой", "*"),
            ("Таблица", "*.csv *.xls *.xlsx"),
            ("Изображение", "*.jpg *.gif *.png"),
            ("Текстовый файл", "*.txt"),
        )
        filepath2 = fd.askopenfilename(
            title="Открыть файл", initialdir="/", filetypes=filetypes
        )
        if filepath2:
            files_to_hash.update({"filepath2": filepath2})
            print("Выбран файл: ", filepath2)
            self.btn_file2.config(bg="green")
            return filepath2
    def start(self):
        self.destroy()
