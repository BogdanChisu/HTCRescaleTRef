from tkinter import *
from PIL import Image, ImageTk
from tkinter import filedialog
from reference_temperature_set import run_all

root = Tk()
im = ImageTk.PhotoImage(Image.open('icons/computer.png'))
root.wm_iconphoto(True, im)

def open():
    root.filename = filedialog.askopenfilename(initialdir = '/home/bogdan/PycharmProjects/work_files',
                                               title = 'Select a csv file',
                                               filetypes = (("csv", "*.csv"),("all files", "*.*")))
    run_all()
    root.quit()


my_btn = Button(root, text="Open a file", command=open).grid(row=0, column=0)

root.mainloop()