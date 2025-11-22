import argparse
import csv
import os
from pathlib import Path
from typing import Iterator, Tuple, Optional, List, Dict

def parse_line(line: str) -> Optional[Tuple[float, float, float, str, str]]:
    line = line.strip()
    if not line:
        return None
    parts = [p.strip() for p in line.split(',')]
    # Need at least 6 parts
    if len(parts) < 6:
        return None
    # Attempt numeric first token
    try:
        lat = float(parts[0])
        lon = float(parts[1])
        altitude = float(parts[3])
    except ValueError:
        return None
    date_token = parts[4]
    time_token = parts[5]
    if len(parts) >= 7 and time_token.count('-') == 2 and parts[6].count(':') == 2:
        # Pattern B with duplicate date before time
        date_token = time_token  # second date
        time_token = parts[6]
    elif time_token.count('-') == 2 and len(parts) >= 7 and parts[6].count(':') == 2:
        # Another variant: parts[5] date, parts[6] time
        date_token = parts[5]
        time_token = parts[6]
    # Basic validation
    if date_token.count('-') != 2 or time_token.count(':') != 2:
        # Not a valid record
        return None
    return lat, lon, altitude, date_token, time_token

def iter_user_files(user_dir: Path) -> Iterator[Path]:
    traj_dir = user_dir / 'Trajectory'
    if not traj_dir.is_dir():
        return
    for f in sorted(traj_dir.glob('*.plt')):
        yield f

def extract_date_prefix_from_filename(filename: str) -> Optional[str]:
    base = os.path.basename(filename)
    if not base.lower().endswith('.plt'):
        return None
    prefix = base[:8]
    if len(prefix) == 8 and prefix.isdigit():
        return prefix
    return None

def merge(root: Path, output_points: Path, daily_summary: Optional[Path] = None) -> None:
    # Prepare writers
    output_points.parent.mkdir(parents=True, exist_ok=True)
    if daily_summary:
        daily_summary.parent.mkdir(parents=True, exist_ok=True)

    point_file = output_points.open('w', newline='', encoding='utf-8')
    point_writer = csv.writer(point_file)
    point_writer.writerow(['User_ID', 'Date', 'Latitude', 'Longitude', 'Altitude', 'Timestamp', 'SourceFile'])

    summary_writer = None
    if daily_summary:
        sf = daily_summary.open('w', newline='', encoding='utf-8')
        summary_writer = csv.writer(sf)
        summary_writer.writerow(['User_ID', 'Date', 'Points', 'StartTime', 'EndTime', 'MinLat', 'MaxLat', 'MinLon', 'MaxLon', 'MinAlt', 'MaxAlt'])
    # Stats accumulator per (user,dateprefix)
    stats: Dict[Tuple[str,str], Dict[str, Optional[float]]] = {}

    for user_dir in sorted(root.iterdir()):
        if not user_dir.is_dir():
            continue
        user_id = user_dir.name
        for plt_path in iter_user_files(user_dir):
            date_prefix = extract_date_prefix_from_filename(plt_path.name)
            # We'll still parse lines for records, using date from content for timestamp.
            try:
                with plt_path.open('r', encoding='utf-8', errors='ignore') as fh:
                    # Skip header lines until numeric start or after 6 lines
                    header_skipped = 0
                    for _ in range(6):
                        line = fh.readline()
                        if not line:
                            break
                        header_skipped += 1
                    # Process remaining lines
                    for line in fh:
                        parsed = parse_line(line)
                        if not parsed:
                            continue
                        lat, lon, altitude, date_token, time_token = parsed
                        # Derive date prefix from either filename or date token
                        date_prefix_content = date_token.replace('-', '')
                        effective_date_prefix = date_prefix if date_prefix else date_prefix_content
                        timestamp = f"{date_token} {time_token}"
                        point_writer.writerow([
                            user_id,
                            effective_date_prefix,
                            f"{lat:.6f}",
                            f"{lon:.6f}",
                            f"{altitude}",
                            timestamp,
                            plt_path.name,
                        ])
                        if summary_writer:
                            key = (user_id, effective_date_prefix)
                            st = stats.get(key)
                            if st is None:
                                st = {
                                    'count': 0,
                                    'start': timestamp,
                                    'end': timestamp,
                                    'min_lat': lat,
                                    'max_lat': lat,
                                    'min_lon': lon,
                                    'max_lon': lon,
                                    'min_alt': altitude,
                                    'max_alt': altitude,
                                }
                                stats[key] = st
                            st['count'] += 1
                            st['end'] = timestamp 
                            st['min_lat'] = min(st['min_lat'], lat)
                            st['max_lat'] = max(st['max_lat'], lat)
                            st['min_lon'] = min(st['min_lon'], lon)
                            st['max_lon'] = max(st['max_lon'], lon)
                            st['min_alt'] = min(st['min_alt'], altitude)
                            st['max_alt'] = max(st['max_alt'], altitude)
            except OSError as e:
                print(f"Failed to read {plt_path}: {e}")
    point_file.close()
    if summary_writer:
        for (user_id, date_prefix), st in sorted(stats.items()):
            summary_writer.writerow([
                user_id,
                date_prefix,
                st['count'],
                st['start'],
                st['end'],
                f"{st['min_lat']:.6f}",
                f"{st['max_lat']:.6f}",
                f"{st['min_lon']:.6f}",
                f"{st['max_lon']:.6f}",
                st['min_alt'],
                st['max_alt'],
            ])
        summary_writer.writerow([])
        summary_writer = None


def main():
    parser = argparse.ArgumentParser(description="Merge Geolife .plt files by daily prefix into a single CSV.")
    parser.add_argument('--root', type=str, default='data_plt', help='Root directory containing user folders (default: Data)')
    parser.add_argument('--output', type=str, default='merged_points.csv', help='Output CSV for detailed points')
    parser.add_argument('--daily-summary', type=str, default=None, help='Optional per-day summary CSV path')
    args = parser.parse_args()

    root_path = Path(args.root).resolve()
    if not root_path.exists():
        raise SystemExit(f"Root path not found: {root_path}")
    output_path = Path(args.output).resolve()
    daily_summary_path = Path(args.daily_summary).resolve() if args.daily_summary else None

    merge(root_path, output_path, daily_summary_path)
    print(f"Detailed points written to: {output_path}")
    if daily_summary_path:
        print(f"Daily summary written to: {daily_summary_path}")

if __name__ == '__main__':
    main()
