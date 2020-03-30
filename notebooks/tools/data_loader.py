import numpy as np
import os

BH_FILE = "bh.dat"
TX_FILE = "tx.dat"
TXIN_FILE = "txin.dat"
TXOUT_FILE = "txout.dat"

LINE_BATCH_SIZE = 500


def load_dat(path, as_ndarray=False, start=0, n_lines="all"):
    """
     Read 'n_lines' lines starting from line 'start' from the file 'path'.
     Return numpy array or list of tuples.
    """
    result_list = []
    with open(path, "r") as fh:
        for i, line in enumerate(fh):
            if i >= start and (n_lines == "all" or n_lines > 0):
                result_list.append(tuple(line.split()))
                if type(n_lines) is not str:
                    n_lines -= 1
    return np.array(result_list) if as_ndarray else result_list


def prepare_subblockchain(data_folder, new_data_folder, start_block, n_blocks):
    """
     Given data in the directory 'data_folder' extract from it a subset of 'n_blocks' blocks
     (and TXs associated with these blocks) starting with block 'start_block.
     Save the new blockchain in the directory 'new_data_folder' with the same structure
     as in 'data_folder'. Basically a vertical reduction of the blockchain.
     For now in only handles the files: BH, TX, TXIN, TXOUT.
    """
    print(f"Trimming {BH_FILE}... ")
    _handle_single_file(
        data_folder, new_data_folder, BH_FILE, start_block, n_blocks, LINE_BATCH_SIZE
    )

    print(f"Trimming {TX_FILE}... ")
    filter_func_tx = (
        lambda line: int(line[1]) >= start_block
        and int(line[1]) <= start_block + n_blocks
    )
    _handle_single_file(
        data_folder,
        new_data_folder,
        TX_FILE,
        start_block,
        n_blocks,
        LINE_BATCH_SIZE,
        filter_func=filter_func_tx,
    )

    filtered_txs = load_dat(
        path=os.path.join(new_data_folder, TX_FILE), start=0, n_lines="all"
    )
    filtered_txs_ids = [t[0] for t in filtered_txs]
    filter_func_tx_full = lambda line: line[0] in filtered_txs_ids

    print(f"Trimming {TXOUT_FILE}... ")
    _handle_single_file(
        data_folder,
        new_data_folder,
        TXOUT_FILE,
        start_block,
        n_blocks,
        LINE_BATCH_SIZE,
        filter_func=filter_func_tx_full,
    )

    print(f"Trimming {TXIN_FILE}... ")
    _handle_single_file(
        data_folder,
        new_data_folder,
        TXIN_FILE,
        start_block,
        n_blocks,
        LINE_BATCH_SIZE,
        filter_func=filter_func_tx_full,
    )
    print("Done.")


def _handle_single_file(
    old_dir,
    new_dir,
    file_name,
    start_block,
    n_blocks,
    batch_size,
    filter_func=lambda x: True,
):
    n_chunks = n_blocks // batch_size
    for j in range(n_chunks):
        _read_and_append_to_new(
            old_dir,
            new_dir,
            file_name,
            start_block + j * batch_size,
            batch_size,
            filter_func,
        )
    leftover = n_blocks % batch_size
    _read_and_append_to_new(
        old_dir, new_dir, file_name, start_block + j * batch_size, leftover, filter_func
    )


def _read_and_append_to_new(old_dir, new_dir, file_name, start, n_lines, filter_func):
    content = load_dat(os.path.join(old_dir, file_name), start=start, n_lines=n_lines)
    with open(os.path.join(new_dir, file_name), "a+") as fh:
        for line in content:
            if filter_func(line):
                fh.write(" ".join(list(line)) + "\n")
