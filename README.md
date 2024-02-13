# LITESOPH

## Engines Interfaced with LITESOPH

[GPAW](https://wiki.fysik.dtu.dk/gpaw/index.html) (version 20.1.0 or later)
> [Installation Instruction](https://wiki.fysik.dtu.dk/gpaw/install.html)

[Octopus](https://octopus-code.org/wiki/Main_Page) (version 11.4)
> [Installation Instruction](https://octopus-code.org/wiki/Manual:Installation)

[NWChem](https://nwchemgit.github.io/) (version 7.0.0 or later)
> [Installation Instruction](https://nwchemgit.github.io/Download.html)

## Requirements

* Python (>3.7)
* Tkinter
* click
* Numpy
* Matplotlib
* Paramiko
* scp
* Rsync

## Installation

clone the main branch and run following in the main project directory

```bash
pip install .
```

## Configuration

To create lsconfig file:

```bash
litesoph config -c
```
  
To edit lsconfig file:

```bash
litesoph config -e
```

### Example lsconfig file

```
[path]
lsproject = <litesoph project path>
lsroot = <installation path of litesoph>

[visualization_tools]
vmd = <path to vmd || e.g. /usr/local/bin/vmd ||can be obtained using :command:`which vmd` >
vesta = <path to vesta || e.g. /usr/local/bin/vesta||can be obtained using :command:`which vesta` >

[engine]
gpaw = <path of gpaw||can be obtained using :command:`which gpaw`> 
nwchem =<binary path of nwchem||can be obtained using :command:`which nwchem`>
octopus =<binary path of octopus ||can be obtained using :command:`which octopus`>


[programs]
python = <path to python||can be obtained using :command:`which python`>

[mpi]
mpirun = <path to mpirun || e.g. /usr/local/bin/mpirun ||can be obtained using :command:`which mpirun`>
gpaw_mpi = <path to mpirun through which gpaw is compiled|| e.g. /usr/local/bin/mpirun>
octopus_mpi =<path to mpirun through which octopus is compiled|| e.g. /usr/local/bin/mpirun>
nwchem_mpi =<path to mpirun through which nwchem is compiled|| e.g. /usr/local/bin/mpirun>
```

The config file follows toml spec.

## Usage

To start gui application, run:

```bash
litesoph gui
```