import subprocess
import os 
import pathlib
from litesoph.config import LSCONFIG

class CLICommand:
    """LITESOPH's graphical user interface.

    """

    @staticmethod
    def add_arguments(parser):
        pass

    @staticmethod
    def run(args):

        
        setup = LSCONFIG()
        setup.setups['lsproject'] = pathlib.Path.cwd()
        
        from litesoph.GUI.gui import AITG
        app = AITG(setup)
        app.title("AITG - LITESOPH")
        app.resizable(True,True)
        app.mainloop()
    