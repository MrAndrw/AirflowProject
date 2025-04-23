import tkinter as tk
import os
from tkinter import *
import tkinter.filedialog as fd
from set_parameters import (files_path as fp, images_path as ip, archives_with_duplicates as awd,
                                 directory as dir, collection_files_path as cfp)
files_path = r""
images_path = r""
archives_with_duplicates = r""
directory = r""
collection_files_path = r""
folders = {"files_path": files_path, "images_path": images_path, "directory": directory,
           "archives_with_duplicates": archives_with_duplicates, "collection_files_path": collection_files_path}


class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.btn_choose_parameters = tk.Button(
            self, text="Нажмите, для использования папок, указанных в параметрах", command=self.choose_parameters
        )
        self.btn_filepath = tk.Button(
            self, text="Выбрать папку для одиночных файлов", command=self.choose_filepath
        )
        self.btn_collection_filepath = tk.Button(
            self, text="Выбрать папку для коллекции файлов", command=self.choose_collection_filepath
        )
        self.btn_imagepath = tk.Button(
            self, text="Выбрать папку для метаинформации", command=self.choose_imagepath
        )
        self.btn_archivepath = tk.Button(
            self, text="Выбрать папку для архивов", command=self.choose_directory
        )
        self.btn_duparchivepath = tk.Button(
            self,
            text="Выбрать папку для переноса повторяющихся архивов",
            command=self.choose_duppath,
        )
        btn_start = tk.Button(
            self, text="Начать работу", width=50, height=2, command=self.start
        )
        self.btn_choose_parameters.pack(padx=100, pady=10)
        self.btn_filepath.pack(padx=100, pady=10)
        self.btn_collection_filepath.pack(padx=100, pady=10)
        self.btn_imagepath.pack(padx=100, pady=10)
        self.btn_archivepath.pack(padx=100, pady=10)
        self.btn_duparchivepath.pack(padx=100, pady=10)
        btn_start.pack(padx=100, pady=10)

    def choose_filepath(self):
        self.btn_filepath.config(bg="white")
        files_path = fd.askdirectory(title="Открыть папку", initialdir="/")
        if files_path:
            print("Папка для файлов: ", files_path)
            folders.update({"files_path": files_path})
            self.btn_filepath.config(bg="green")
            return files_path

    def choose_parameters(self):
        self.btn_choose_parameters.config(bg="white")
        files_path = fp
        collection_files_path = cfp
        images_path = ip
        directory = dir
        archives_with_duplicates = awd

        print("Папка для файлов: ", files_path)
        print("Папка для коллекций файлов: ", collection_files_path)
        print("Папка для метаинформации: ", images_path)
        print("Папка для архивов: ", directory)
        print("Папка для повторяющихся архивов: ", archives_with_duplicates)

        folders.update({"files_path": files_path.replace('\'', "")})
        folders.update({"collection_files_path": collection_files_path.replace('\'', "")})
        folders.update({"images_path": images_path.replace('\'', "")})
        folders.update({"directory": directory.replace('\'', "")})
        folders.update({"archives_with_duplicates": archives_with_duplicates.replace('\'', "")})
        self.btn_choose_parameters.config(bg="green")

        return files_path

    def choose_collection_filepath(self):
        self.btn_collection_filepath.config(bg="white")
        collection_files_path = fd.askdirectory(title="Открыть папку", initialdir="/")
        if collection_files_path:
            print("Папка для коллекций файлов: ", collection_files_path)
            folders.update({"collection_files_path": collection_files_path})
            self.btn_collection_filepath.config(bg="green")
            return collection_files_path

    def choose_imagepath(self):
        self.btn_imagepath.config(bg="white")
        images_path = fd.askdirectory(title="Открыть папку", initialdir="/")
        if images_path:
            print("Папка для метаинформации: ", images_path)
            folders.update({"images_path": images_path})
            self.btn_imagepath.config(bg="green")
            return images_path

    def choose_directory(self):
        self.btn_archivepath.config(bg="white")
        directory = fd.askdirectory(title="Открыть папку", initialdir="/")
        if directory:
            print("Папка для архивов: ", directory)
            os.chdir(directory)
            folders.update({"directory": directory})
            self.btn_archivepath.config(bg="green")
            return directory

    def choose_duppath(self):
        self.btn_duparchivepath.config(bg="white")
        archives_with_duplicates = fd.askdirectory(
            title="Открыть папку", initialdir="/"
        )
        if archives_with_duplicates:
            print("Папка для повторяющихся архивов: ", archives_with_duplicates)
            folders.update({"archives_with_duplicates": archives_with_duplicates})
            self.btn_duparchivepath.config(bg="green")
            return archives_with_duplicates

    def start(self):
        self.destroy()
    # def test(self):
    #     window = Tk()
    #     window.title("Приложение")
    #     lbl = Label(window, text="Привет")
    #     lbl.grid(column=10, row=10)
    #     window.mainloop()

# print(t)
# app = App()
# app.mainloop()
# print(t)
# print(t.get(0))
# d = app.choose_filepath()
# print(d)
