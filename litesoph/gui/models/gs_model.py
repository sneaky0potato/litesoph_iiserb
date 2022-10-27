from tkinter import messagebox

def choose_engine(input_param:dict):
        for basis_key in ['basis_type:common','basis_type:extra']:
                if input_param.get(basis_key) is not None:                        
                        basis = input_param.get(basis_key)
        if basis is None:
                raise KeyError("No match for basis key")

        boxshape = input_param.get('box shape')
        if boxshape:
                if basis == "LCAO":
                        if boxshape == "parallelepiped":
                                engine = "gpaw"
                                return engine
                        else:
                                messagebox.showinfo(message="Not Implemented")
                                return
                if basis == "FD":
                        if boxshape == "parallelepiped":
                                check = messagebox.askyesno(title = 'Message',message= "The default engine for the input is gpaw, please click 'yes' to proceed with it. If no, octopus will be assigned")
                                if check is True:
                                        engine = "gpaw"
                                elif check is False:
                                        engine = "octopus"
                        elif boxshape in ["minimum","sphere","cylinder"] : 
                                engine = "octopus"
                        return engine
        else:
                if basis == "PW":
                        engine = "gpaw"
                elif basis == "Gaussian":
                        engine = "nwchem"
                return engine

def get_inp_summary(gui_input:dict):
        pass