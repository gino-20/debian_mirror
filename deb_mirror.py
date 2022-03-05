# Version 3_b_250821 (ReadBlocksTM)
import urllib3
import os
from random import randint
import argparse
import hashlib
from colorama import Fore
from multiprocessing import Pool
from tqdm import tqdm


class RepoDownloader:
    def __init__(self):
        try:
            self.threads = int(args.threads)
        except ValueError:
            print('Threads count must be integer!')
            exit()
        self.base_url = args.url
        self.release_name = args.release
        self.branches = {'main': args.main, 'multiverse': args.multiverse, 'restricted': args.restricted,
                         'universe': args.universe}
        if args.custom:
            self.branches.update({branch: True for branch in args.custom.split(' ')})
        self.release_arch = args.architecture
        # Set download buffer to match kernel setting
        # with open('/proc/sys/net/core/rmem_default', 'r') as buffer:
        self.buffer = 4096
        print(f'Download buffer set to {self.buffer}')
        self.t_folder_name = './tmp' + str(randint(10000000, 99999999))
        if args.lfolder.endswith('/'):
            self.local_repo_folder = args.lfolder + 'deb/'
            self.local_index_folder = args.lfolder
        else:
            self.local_repo_folder = args.lfolder + '/deb/'
            self.local_index_folder = args.lfolder + '/'

        self.total_size = 0
        if args.check:
            self.existing_check()
        else:
            self.package_urls = []
            self.scan_repo()
            self.index_packages()
            self.download_packages()
            if args.clean:
                self.find_diffs()

    def beautiful_print(self, fname, result):
        lmarg = 79-len(fname)
        if result == 'OK':
            print(f'{fname}{"-" * lmarg}[{Fore.GREEN}{result: ^12}{Fore.RESET}]')
        elif result == 'Mismatch':
            print(f'{fname}{"-" * lmarg}[{Fore.RED}{result: ^12}{Fore.RESET}]')
        elif result == 'FileNotFound':
            print(f'{fname}{"-" * lmarg}[{Fore.YELLOW}{result: ^12}{Fore.RESET}]')
        else:
            print(f'{fname}{"-" * lmarg}[{result: ^12}]')

    def existing_check(self):
        # Resume previous download session, update current repo or check integrity
        sum_index = {}
        auto_remove = False
        total_missmatch = total_not_found = 0
        not_found_list = []
        mismatch_list = []

        if os.path.exists(self.local_repo_folder) and os.path.exists(self.local_index_folder+'Packages'):
            print('Found previous archive, starting integrity check...')
            with open(self.local_index_folder+'Packages', 'r') as local_index:
                print('Caching local index...')
                # Read one file-block of the index
                line_index = ''.join(local_index.readlines()).split('\n\n')
                c = len(line_index)
                print(f'{c} packages to check...')
                print('Indexing packages...')
                for block in line_index:
                    print(f'{c} packages remaining...\r', end='')
                    for line in block.split('\n'):
                        if line.startswith('Filename: '):
                            sum_index.update({line.split(' ')[-1]: 0})
                            break
                    for line in block.split('\n'):
                        if line.startswith('MD5sum'):
                            sum_index[list(sum_index.keys())[-1]] = line.split(' ')[-1]
                            break
                    c -= 1
            for file in sum_index.keys():
                file_hash = hashlib.md5()
                try:
                    with open(self.local_index_folder+file, 'rb') as deb:
                        for chunk in iter(lambda: deb.read(4096), b""):
                            file_hash.update(chunk)
                except FileNotFoundError:
                    self.beautiful_print(file, 'FileNotFound')
                    not_found_list.append(file)
                    total_not_found += 1
                else:
                    if file_hash.hexdigest() == sum_index[file]:
                        self.beautiful_print(file, 'OK')
                    else:
                        self.beautiful_print(file, 'Mismatch')
                        mismatch_list.append(file)
                        total_missmatch += 1
                        if not auto_remove:
                            while (inp := input('Remove the broken DEB? (y/n/a) a=remove all:  ')) \
                                    not in ['y', 'n', 'a']:
                                print('Yes or NO?!')
                            if inp == 'y':
                                os.remove(self.local_index_folder+file)
                            elif inp == 'n':
                                continue
                            else:
                                os.remove(self.local_index_folder + file)
                                auto_remove = True
                        else:
                            os.remove(self.local_index_folder + file)
        print(f'Total not found: {total_not_found}')
        for index, value in enumerate(not_found_list, 1):
            print(index, value)
        print(f'Total missmatch: {total_missmatch}')
        for index, value in enumerate(mismatch_list, 1):
            print(index, value)

    # Scan the Repo for the latest indexes
    def scan_repo(self):
        # Make temp folder
        os.mkdir(self.t_folder_name)
        http = urllib3.PoolManager()
        # Get Packages.xz from all active branches
        if os.path.exists(self.local_index_folder+'Packages'):
            print('Updating local Release index...')
            os.remove(self.local_index_folder+'Packages')
        for b in self.branches:
            if self.branches[b]:
                print(f'Downloading index for {b}')
                os.mkdir(self.t_folder_name + '/' + b)
                r = http.request('GET', self.base_url + '/dists/' + self.release_name + '/' + b + '/'
                                 + self.release_arch + '/' + 'Packages.gz',
                                 preload_content=False)
                with open(self.t_folder_name + '/' + b + '/' + 'Packages.gz', 'wb') as file:
                    while True:
                        t_data = r.read(self.buffer)
                        if not t_data:
                            break
                        file.write(t_data)
                    r.release_conn()
                # Decompressing indexes
                os.system('gunzip ' + self.t_folder_name + '/' + b + '/' + 'Packages.gz')

    # Let's parse indexes and build the full list of packages to download
    def index_packages(self):
        # Local index initialisation
        for b in self.branches:
            if self.branches[b]:
                with open(self.t_folder_name + '/' + b + '/' + 'Packages', 'r') as index, \
                        open(self.local_index_folder+'Packages', 'a') as local_index:
                    print(f'Caching index for {b}...')
                    index_cache = index.readlines()
                    print(f'{len(index_cache)} strings found...')
                    for block in ''.join(index_cache).split('\n\n'):
                        for line in block.split('\n'):
                            if line.startswith('Filename: '):
                                # Check for previous download sessions
                                if not os.path.exists(self.local_repo_folder +
                                                                     line.replace('\n', '').split('/')[-1]):
                                    self.package_urls.append(self.base_url + '/' + line.replace('\n', '')[10:])
                                    # Now let's add do some size calculations...
                                    for bline in block.split('\n'):
                                        if bline.startswith('Size:'):
                                            self.total_size += int(bline.split(': ')[1])
                                    # Let's write modified line to the local index
                                local_index.write('Filename: ' + 'deb/' + line.split('/')[-1] + '\n')
                            else:
                                local_index.write(line + '\n')
                        local_index.write('\n')
        print()
        print(f'Total {len(self.package_urls)} packages to download, total size: {self.total_size/10**9:.1f}Gb')
        print(f'Downloading to {self.local_repo_folder}...')
        os.system('rm -rf '+self.t_folder_name)

    def find_diffs(self):
        file_list = []
        index_list = []
        delete_list = []
        print('Cleaning obsolete packages...')
        print('Caching file list...')
        with os.scandir(self.local_repo_folder) as entries:
            for file in entries:
                file_list.append(file.name)
        print(f'Total {len(file_list)} found...')
        print('Caching local index...')
        with open(self.local_index_folder+'Packages', 'r') as index:
            while line := index.readline():
                if line.startswith('Filename: '):
                    index_list.append(line.replace('\n', '').split(' ')[-1])
        print(f'Total {len(index_list)} entries found...')
        for entrie in file_list:
            if ('deb/'+entrie) not in index_list:
                delete_list.append(entrie)
        print(f'Total {len(delete_list)} files to remove...')
        for entrie in delete_list:
            os.remove(self.local_repo_folder+entrie)
            self.beautiful_print(entrie, 'Done')
        if len(index_list) > len(file_list):
            print('Looks like file list doesnt match index...')
            while (answer := input('Do You like to try to find missing packages? (y/n): ')) not in ['y', 'n']:
                print('Yes or No')
            if answer == 'y':
                print('Looking for missing ones...')
                for file in file_list:
                    try:
                        index_list.pop(index_list.index('deb/'+file))
                    except ValueError:
                        print('Total file mismatch... You should delete current index and invoke repo update...')
                # Now we must have only missing entries in index_list
                # Let's create new task for downloader...
                print(f'Total {len(index_list)} missing ones...')
                for index, value in enumerate(index_list, 1):
                    print(index, value)
            else:
                return

    def download_packages(self):
        if not os.path.exists(self.local_repo_folder):
            os.mkdir(self.local_repo_folder)
        with Pool(processes=int(args.threads)) as p:
            r = list(tqdm(p.imap(self.download_thread, self.package_urls),
                     total=len(self.package_urls), unit=' packages'))

    def download_thread(self, url):
        http = urllib3.PoolManager()
        fname = url.split('/')[-1]
        r = http.request('GET', url, preload_content=False)
        if args.verbose:
            print(f'Downloading {fname}, size: {r.info()["Content-Length"]}')
        try:
            with open(self.local_repo_folder + fname, 'wb') as file:
                while True:
                    t_data = r.read(self.buffer)
                    if not t_data:
                        break
                    file.write(t_data)
                r.release_conn()
        except KeyboardInterrupt:
            print('Download aborted! Removing partial downloaded file...')
            os.remove(self.local_repo_folder + fname)
            print(f'File {fname} successfully deleted!')
        finally:
            return


message = '\nDone!\nDon\'t forget to add \"deb [allow-insecure=yes] ' \
          'file:///<path_to_your_repo_dir> ./\" to your sources.list'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Debian-based repo downloader',
                                     usage='Enter the base url, branches and local folder name')
    parser.add_argument('-url', type=str, help='Base url (default = http://mirror.linux-ia64.org/ubuntu/)',
                        default='http://mirror.linux-ia64.org/ubuntu/')
    parser.add_argument('-lfolder', type=str, help='Local folder to store repo (default=./repo/)', default='./repo/')
    parser.add_argument('-release', type=str, help='Release name (default = focal)', default='focal')
    parser.add_argument('-arch', '--architecture', type=str, help='Architecture to use (default=binary-amd64)',
                        default='binary-amd64')
    parser.add_argument('-m', '--main', action='store_true', help='main branch')
    parser.add_argument('-mul', '--multiverse', action='store_true', help='Multiverse branch')
    parser.add_argument('-r', '--restricted', action='store_true', help='Restricted brach')
    parser.add_argument('-u', '--universe', action='store_true', help='Universe brach')
    parser.add_argument('-v', '--verbose', action='store_true', help='Be more verbose')
    parser.add_argument('-c', '--custom', type=str, help='Custom branch')
    parser.add_argument('--clean', action='store_true', help='Auto clean obsolete packages')
    parser.add_argument('--check', action='store_true', help='Check existing repo for integrity')
    parser.add_argument('--threads', type=str, help='Number of simultaneous downloads', default='1')
    args = parser.parse_args()
    if not (args.main or args.multiverse or args.restricted or args.universe or args.custom or args.check):
        parser.print_help()
        exit()
    RepoDownloader()
    with open(args.lfolder + '/read.me', 'w') as readme:
        readme.write(message+'\n')
    print(message)
