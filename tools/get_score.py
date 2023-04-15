import orjson
import re
import math
import sys
import pandas as pd


def get_score():
    FS = 10 * 1024  # file size (KB)
    bitrate = 1 * 1024 * 1024  # bitrate (bps)
    DT = FS * 1024 * 8 / bitrate  # duration (s)

    reg_find_json = r"([{].*?[}])"
    pair = re.compile(reg_find_json)
    rawlogs = []
    with open(path_trace) as file:
        for line in file.readlines():
            search_rt = pair.search(line)
            if search_rt is not None:
                rawlogs.append(orjson.loads(search_rt.group(0)))
    raw_data_pd = pd.DataFrame(rawlogs)
    tx_data_pd = raw_data_pd[raw_data_pd['event'] == 'Tx'].copy()
    tx_data_pd.sort_values('value', inplace=True)
    rx_data_pd = raw_data_pd[raw_data_pd['event'] == 'Rx'].copy()
    rx_data_pd.sort_values('value', inplace=True)
    send_subpiece = tx_data_pd[['timestamp', 'value']].to_numpy()
    recv_subpiece = rx_data_pd[['timestamp', 'value']].to_numpy()

    if len(recv_subpiece) == 0:
        print(f'data loss! Score: 0')
        return 0
    recv_ts = {}
    for item in recv_subpiece:
        recv_ts[item[1]] = item[0] if item[1] not in recv_ts.keys() else min(recv_ts[item[1]], item[0])

    ts_tmp = 0
    for seq, ts in recv_ts.items():
        ts_tmp = max(ts, ts_tmp)
        recv_ts[seq] = ts_tmp

    t_start = send_subpiece[0, 0]
    recv_final_idx = recv_subpiece[-1, 1]
    recv_final_ts = recv_ts[recv_final_idx]
    print(f'recv_start_ts: {t_start}')
    print(f'recv_end_ts: {recv_final_ts}')
    T = (recv_final_ts - t_start) / 1e6
    DS_ = len(recv_ts.keys())
    DS = len(recv_subpiece) / 1024

    if DS_ < FS:
        print(f'not recv enough data! File size: {FS / 1024} MB, total data downloaded: {DS} MB, total data '
              f'downloaded (no duplicate): {DS_ / 1024} MB \n')
        return 0

    if T > DT:
        print(f"Timeout！video duration: {DT} s, download duration: {T} s, total data downloaded: {DS} MB, "
              f"total data downloaded (no duplicate): {DS_ / 1024} MB \n")
        return 0

    print(
        f"\nFile size: {FS / 1024} MB, video duration: {DT} s, download duration: {T} s, total data downloaded: {DS}"
        f" MB, total data downloaded (no duplicate): {DS_ / 1024} MB \n")
    # check every 10 sec
    t_sum = 0
    for i in range(math.floor(DT / 10)):
        data_required = 10 * (i + 1) * bitrate / 8 / 1024
        subpiece_num = math.ceil(data_required)
        if subpiece_num > len(recv_ts.keys()):
            print("not receive enough data!")
            return 0

        t = (recv_ts[subpiece_num - 1] - t_start) / 1e6
        t_sum += max(0, t - 10 * (i + 1))
        print(f'Video duration: {10 * (i + 1)} s, amount of data required: {data_required} KB, '
              f'download duration: {t} s, amount of pieces downloaded: {subpiece_num}')

    alpha = (DS * 1024 + FS) / (2 * FS)
    belta = t_sum / T
    Score = round(float(FS / ((alpha + belta) * T)), 10)
    print(f'\nalpha: {alpha}, belta: {belta}, Score: {Score} KBps \n')
    return Score


if __name__ == '__main__':
    if len(sys.argv) == 1 or len(sys.argv) == 0:
        print(f'please input the path of trace log!')
    else:
        path_trace = sys.argv[1]
        get_score()
