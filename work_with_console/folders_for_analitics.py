import tkinter as tk
import os
from tkinter import *
import tkinter.filedialog as fd

work_folder = r"D:\analitics"
analitic_folders = {
    "work_folder": work_folder,
}


class Analitic_App(tk.Tk):

    def __init__(self):
        super().__init__()

        self.btn_folder1 = tk.Button(
            self, text="Выбрать папку для сохранения отчета", command=self.choose_work_folder
        )
        btn_start = tk.Button(
            self, text="Начать работу", width=50, height=2, command=self.start
        )
        self.btn_folder1.pack(padx=100, pady=10)
        btn_start.pack(padx=100, pady=10)

    def choose_work_folder(self):
        self.btn_folder1.config(bg="white")
        work_folder = fd.askdirectory(title="Открыть папку", initialdir="/")
        if work_folder:
            print("Папка для работы: ", work_folder)
            analitic_folders.update({"work_folder": work_folder})
            self.btn_folder1.config(bg="green")
            return work_folder

    def start(self):
        self.destroy()
