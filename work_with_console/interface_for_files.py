import tkinter as tk
import tkinter.filedialog as fd


class App2(tk.Tk):
    files = []
    collection_folders = []

    def __init__(self):
        super().__init__()
        self.btn_file = tk.Button(self, text="Выбрать отдельный файл", command=self.choose_file)
        self.btn_collection_filepath = tk.Button(
            self, text="Выбрать папку с файлами", command=self.choose_collection_filepath
        )
        self.btn_start = tk.Button(
            self, text="Завершить выбор файлов", width=50, height=2, command=self.start
        )
        self.btn_file.pack(padx=100, pady=10)
        self.btn_collection_filepath.pack(padx=100, pady=10)
        self.btn_start.pack(padx=100, pady=10)

    def choose_file(self):
        self.btn_file.config(bg="white")
        filetypes = (
            ("Любой", "*"),
            ("Таблица", "*.csv *.xls *.xlsx"),
            ("Изображение", "*.jpg *.gif *.png"),
            ("Текстовый файл", "*.txt"),
        )
        filepath = fd.askopenfilename(
            title="Открыть файл", initialdir="/", filetypes=filetypes
        )
        if filepath:
            App2.files.append(filepath)
            print("Выбран файл: ", filepath)
            self.btn_file.config()
            return App2.files, App2.collection_folders

    def choose_collection_filepath(self):
        self.btn_collection_filepath.config(bg="white")
        collection_files_path = fd.askdirectory(title="Открыть папку", initialdir="/")
        if collection_files_path:
            print("Папка с файлами: ", collection_files_path)
            App2.collection_folders.append(collection_files_path)
            self.btn_collection_filepath.config()
            return App2.files, App2.collection_folders

    def start(self):
        self.destroy()
