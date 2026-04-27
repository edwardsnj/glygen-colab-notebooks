
import os, os.path
import urllib.request
import re, fnmatch

import pandas as pd

version = "1.0.0"

class GlyGenDownloader(object):
  _base = "https://data.glygen.org/ln2data/releases/data/current/reviewed/"
  _cache = ".glygen"
  
  glygentaxid = {
    "human": 9606,
    "mouse": 10090,
    "rat": 10116,
    "arabidopsis": 3702,
    "bovine": 9913,
    "chicken": 9031,
    "dicty": 44689,
    "fruitfly": 7227,
    "hamster": 10029,
    "hcv1a": 11108,
    "hcv1b": 11116,
    "pig": 9823,
    "sarscov1": 694009,
    "sarscov2": 2697049,
    "yeast": 4932,
    "zebrafish": 7955,
  }

  def __init__(self,usecache=True,verbose=True):
    self.verbose = verbose
    self.usecache = usecache

  def file_size(self,filename,units=None):
    size_bytes = os.path.getsize(filename)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if (size_bytes < 1024 and units is None) or (unit == units):
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"
  
  anchorre = re.compile(r'<a href="([^"]*)">([^<]*)</a>')

  def filenames(self,pattern,**kwargs):
    glob = pattern.format(**kwargs)

    fns = []
    page = urllib.request.urlopen(self._base).read().decode()
    for m in self.anchorre.finditer(page):
      fn = m.group(1)
      if fn.endswith('.stat.csv'):
        continue
      if fnmatch.fnmatch(fn,glob):
        fns.append(fn)
    return sorted(fns)

  def download(self,filename,todir=None):
    if todir is None:
      todir = self._cache
    else:
      todir = todir.rstrip(os.sep)
    todir += os.sep
    os.makedirs(todir,exist_ok=True)
    if not self.usecache or not os.path.exists(todir + filename):
      if self.verbose:
        print(f"Download {filename}...", end="")
      if os.path.exists(todir + filename):
        os.unlink(todir + filename)
      urllib.request.urlretrieve(self._base + filename, todir + filename)
      if self.verbose:
        print(f" done ({self.file_size(todir + filename)}).")
    else:
      if self.verbose:
        print(f"Using cached {filename} ({self.file_size(todir + filename)}).")
    return todir + filename
  
  def dataframe(self,*filenames,usecols=None,notna=None,asint=None,
                                setcolumn=None,transform=None,
                                dropcols=None,dropdups=False,
                                addfilename=False,addspecies=False,
                                addtaxid=False,filterrows=None):
    dfs = []
    if isinstance(filenames[0],list) and len(filenames) == 1:
      filenames = filenames[0]
    for fn in filenames:
      if not os.path.exists(fn):
        fn = self.download(fn)
      for df in pd.read_csv(fn,usecols=usecols,chunksize=100000):
        if notna is not None:
          for colname in notna:
            df = df[~df[colname].isna()]
        if asint is not None:
          for colname in asint:
            df[colname] = df[colname].astype(int)
        if setcolumn is not None:
          for k,v in setcolumn.items():
            df[k] = v
        filename = os.path.split(fn)[1]
        species = os.path.split(fn)[1].split("_",1)[0]
        if addfilename:
          df['filename'] = os.path.split(fn)[1]
        if addspecies:
          if species in self.glygentaxid:
            df['species'] = species
        if addtaxid:
          if species in self.glygentaxid:
            df['taxid'] = self.glygentaxid[species]
        if transform is not None:
          for k,v in transform.items():
            df[k] = v(df)
        if filterrows is not None:
          for f in filterrows:
            df = df[f(df)]
        if dropdups:
          df = df.drop_duplicates()
        dfs.append(df)
    df = pd.concat(dfs,ignore_index=True)
    df.reset_index(inplace=True,drop=True)
    if dropcols is not None:
      df = df.drop(columns=dropcols)
    if dropdups:
      df = df.drop_duplicates()
    if self.verbose:
      print("Constructed data-frame:\n")
      df.info()
      print()
    return df

  def cached_dataframe(self,name,*filenames,**kwargs):
    filename = os.path.join(self._cache,"_" + name + ".csv")
    if os.path.exists(filename):
      print(f"Reading cached data-frame {name}...", end="")
      df = pd.read_csv(filename)
      print(f"done. ({df.shape[0]} rows)\n")
      df.info()
      print()
    else:
      df = self.dataframe(*filenames,**kwargs)
      df.to_csv(filename,index=False)
    return df 

if __name__ == "__main__":

  ggdl = GlyGenDownloader()

  site_files = ggdl.filenames("{species}_proteoform_glycosylation_sites_*.csv",species="human")

  print(site_files)

    
