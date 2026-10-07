#!/usr/bin/env python3
"""Digest-pinned regular-file transfer between isolated release jobs."""
import argparse
from pathlib import Path
import zipfile
import shutil
from import_release_bundle import extract
from release_candidate import regular_files
from release_common import require, sha256


def pack(root, output):
    root=Path(root).resolve(); output=Path(output).absolute()
    require(not output.is_relative_to(root),'Transfer archive cannot be inside its input directory')
    output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(output,'x',zipfile.ZIP_DEFLATED) as archive:
        for path in regular_files(root):
            info=zipfile.ZipInfo(path.relative_to(root).as_posix(),(1980,1,1,0,0,0))
            info.create_system=3; info.external_attr=0o100644<<16
            info.compress_type=zipfile.ZIP_STORED if path.suffix in {'.gz','.zip','.jar'} else zipfile.ZIP_DEFLATED
            with path.open('rb') as reader, archive.open(info,'w',force_zip64=True) as writer:
                shutil.copyfileobj(reader,writer,1024*1024)
    return sha256(output)


def unpack(archive, expected, output):
    require(not Path(archive).is_symlink() and sha256(archive)==expected,'Release transfer digest mismatch')
    extract(archive,Path(output),require_manifest=False)
    require(sha256(archive)==expected,'Release transfer changed during extraction')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__); commands=parser.add_subparsers(dest='operation',required=True)
    make=commands.add_parser('pack'); make.add_argument('--source',type=Path,required=True); make.add_argument('--archive',type=Path,required=True)
    read=commands.add_parser('unpack'); read.add_argument('--archive',type=Path,required=True); read.add_argument('--sha256',required=True); read.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.operation=='pack': print(pack(args.source,args.archive))
    else: unpack(args.archive,args.sha256,args.output)
