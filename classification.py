import sqlite3
import os
import random
import re
import csv

RANK_POINTS = [25,20,18,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,1,1,1,1,1,1,1,1,1,1]
pilots_results = {}
IN_FOLDER = "in_databases"
OUT_FOLDER = "out_data"
PRINT_HEAT_RESULTS = True

def sorted_nicely(l):
    """ Sort the given iterable in the way that humans expect."""
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    return sorted(l, key = alphanum_key)


class Pilot:
    def __init__(self, nick):
        self.nick = nick # Nick in the database
        self.points = 0 # Number of points of this pilot

        # Tiebraker conditions, apply in this order
        self.won_races = 0 # Number of won races of this pilot
        self.consecutives_3_fastest_laps = 0 # Number of 3 fastest consecutive laps in a race
        self.race_position_accomulator = 0 # Accomulator adding position of each race
        self.completed_laps = 0 # Total number of completed laps # I think it's a count on saved_race_lap where deleted = 0 and substract 1 for the first pass
        self.fastest_race = 0 # Number of times this pilot has had the fastest overall race (needs to complete all laps)
        self.fastest_laps = 0 # Number of fastest laps throught the league
        self.coin_flip = random.random() # If after all is still a tie, coin flip

def get_sorted_heat_results(cursor: sqlite3.Cursor, race_id, heat_id):
    # Obtain pilots in that race
    cursor.execute('SELECT pilot_id FROM saved_pilot_race WHERE race_id = ?', (race_id,))
    pilots = cursor.fetchall()

    pilot_results = []

    # For each pilot in the race
    for (pilot_id,) in pilots:
        # Get laptimes
        cursor.execute(
            'SELECT lap_time FROM saved_race_lap WHERE race_id = ? AND pilot_id = ? AND deleted = 0',
            (race_id, pilot_id)
        )
        laps = cursor.fetchall()

        # Save number of laps and lap time for that pilot
        number_of_laps = len(laps)
        race_time = sum(lap[0] or 0 for lap in laps)
        pilot_results.append({
            'pilot_id': pilot_id,
            'number_of_laps': number_of_laps,
            'race_time': race_time
        })

    # Sort first by number of laps and then by lap time
    pilot_results.sort(key=lambda x: (-x['number_of_laps'], x['race_time']))

    return {
        'heat_id': heat_id,
        'pilots': pilot_results
    }

def msec_to_min_sec_dec(ms):
    if ms is None:
        return f"None"

    sec = ms/1000.0

    min = int(sec // 60)
    sec_restantes = sec % 60

    return f"{min}:{sec_restantes:06.3f}"


def get_fastest_X_consecutive(cursor: sqlite3.Cursor, race_id, n_consecutive_laps=3):
    # Obtain pilots in that race
    cursor.execute('SELECT pilot_id FROM saved_pilot_race WHERE race_id = ?', (race_id,))
    pilots = cursor.fetchall()

    pilot_results = {}

    # For each pilot in the race
    for (pilot_id,) in pilots:
        cursor.execute('SELECT callsign, name FROM pilot WHERE id=?', (pilot_id,))
        pilot_nick, pilot_name = cursor.fetchone()

        # Initialize result
        pilot_results[pilot_nick] = None

        # Get laptimes
        cursor.execute(
            'SELECT lap_time FROM saved_race_lap WHERE race_id = ? AND pilot_id = ? AND deleted = 0',
            (race_id, pilot_id)
        )

        laps = cursor.fetchall()

        # Convert to list of floats
        laptimes = [t[0] for t in laps]

        # At least it must have 2 laps to start counting consecutive lap times
        if len(laptimes) < 2:
            continue

        # Add first pass to start/finish line to first lap
        #laptimes = [laptimes[0]+laptimes[1]] + laptimes[2:]

        # Rotorhazard does not take into account first pass on start/finish line, do the same for now
        laptimes = laptimes[1:]

        if len(laptimes) < n_consecutive_laps:
            continue

        # Initialzie best time to infinite
        best_time = float("inf")

        for i in range(len(laptimes) - n_consecutive_laps + 1):
            accomulated_laptime = sum(laptimes[i:i + n_consecutive_laps])
            if accomulated_laptime < best_time:
                best_time = accomulated_laptime

        pilot_results[pilot_nick] = best_time

    return pilot_results


def compute_race_points(conn: sqlite3.Connection):
    # Connect to the database
    c = conn.cursor()
    
    # Get the last race class
    c.execute('SELECT MAX(id), name FROM race_class')
    last_raceclass_id, last_raceclass_name = c.fetchall()[0]
    print("\tLast raceclass is "+last_raceclass_name+" with id "+str(last_raceclass_id))

    # Get all heat ids from that race class
    c.execute('SELECT heat_id FROM saved_race_meta WHERE class_id=? ORDER BY heat_id DESC', (last_raceclass_id, ))
    last_raceclass_heat_ids = c.fetchall()

    # Position in this race
    position = 1

    # Iterate over each heat id
    for heat_id in last_raceclass_heat_ids:
        current_heat_id = heat_id[0]

        # Get the race id for that heat
        c.execute('SELECT id FROM saved_race_meta WHERE heat_id=?', (current_heat_id, ))
        race_id = c.fetchall()[0][0]
        if PRINT_HEAT_RESULTS: print(f'\n\theat_id: {heat_id} (race_id: {race_id})')

        # Get times of all pilots in that heat
        sorted_pilots = get_sorted_heat_results(c, race_id, heat_id)

        # Get pilot name
        for pilot in sorted_pilots['pilots']:
            pid = pilot['pilot_id']
            c.execute('SELECT callsign, name FROM pilot WHERE id=?', (pid,))
            pilot_nick, pilot_name = c.fetchone()
            
            # If pilot is P1, increase counter of won races
            if position == 1:
               pilots_results[pilot_nick].won_races += 1
            
            pilots_results[pilot_nick].race_position_accomulator += position
            points = RANK_POINTS[position-1]
            pilots_results[pilot_nick].points += points
            if PRINT_HEAT_RESULTS: print(f'\t\tPos {position}: {pilot_nick} ({pilot_name}) [pilot_id={pid}]')
            position+=1

def compute_number_of_laps(conn: sqlite3.Connection):
    # Connect to the database
    c = conn.cursor()
    
    # Get number of laps per pilot. Filtering out deleted laps and the first lap of each pilot in each heat
    query = """
    SELECT pilot_id, COUNT(*) AS total_laps
    FROM saved_race_lap
    WHERE deleted = 0
    AND (pilot_id, race_id, lap_time_stamp) NOT IN (
        SELECT pilot_id, race_id, MIN(lap_time_stamp)
        FROM saved_race_lap
        WHERE deleted = 0
        GROUP BY pilot_id, race_id
    )
    GROUP BY pilot_id
    ORDER BY total_laps DESC;
    """

    # Run the query
    c.execute(query)

    # Get results
    results = c.fetchall()

    # Save and show results
    print("Number of laps:")
    for pilot_id, number_of_laps in results:
        # Get callsign of pilot
        c.execute('SELECT callsign FROM pilot WHERE id=?', (pilot_id,))
        pilot_nick = c.fetchone()[0]
        pilots_results[pilot_nick].completed_laps = number_of_laps
        print(f"Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Total Laps: {number_of_laps}")
    print("Number of laps end")


def compute_fastest_lap(conn: sqlite3.Connection):
    c = conn.cursor()

    # This query will get the fastest lap of each heat
    query = """
    SELECT pilot_id, race_id, lap_time, lap_time_formatted, lap_time_stamp
    FROM saved_race_lap
    WHERE deleted = 0
    AND (pilot_id, race_id, lap_time_stamp) NOT IN (
        SELECT pilot_id, race_id, MIN(lap_time_stamp)
        FROM saved_race_lap
        WHERE deleted = 0
        GROUP BY pilot_id, race_id
    )
    ORDER BY lap_time ASC, race_id ASC, lap_time_stamp ASC; -- Sort first by lap_time and if two pilots have the same lap time, the fastest lap goes to the first one to do it. First sort by race id and then by stamp, as the stamp is relative per each race id
    """

    # Run the query
    c.execute(query)

    # Get first fastest lap
    pilot_id, race_id, fastest_lap, fastest_lap_formatted, lap_time_stamp = c.fetchone()

    # Get nick of the pilot
    c.execute('SELECT callsign FROM pilot WHERE id=?', (pilot_id,))
    pilot_nick = c.fetchone()[0]

    # Save and show fastest lap
    pilots_results[pilot_nick].fastest_laps += 1
    print(f"Fastest lap: Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Formatted lap time: {fastest_lap_formatted}, Race ID: {race_id}, Fastest lap: {fastest_lap}, Lap Time Stamp {lap_time_stamp}")

    # Show all
    # # Obtain all results
    # results = c.fetchall()

    # # Show all results
    # print("Fastest lap")
    # for pilot_id, race_id, fastest_lap, fastest_lap_formatted, lap_time_stamp in results:
    #     # Get callsign of pilot
    #     c.execute('SELECT callsign FROM pilot WHERE id=?', (pilot_id,))
    #     pilot_nick = c.fetchone()[0]
    #     print(f"Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Formatted lap time: {fastest_lap_formatted}, Race ID: {race_id}, Fastest lap: {fastest_lap}, Lap Time Stamp {lap_time_stamp}")
    # print("End fastest lap")

def compute_fastest_race(conn: sqlite3.Connection):
    c = conn.cursor()
    # Get fastest overall race time, but sorting first by those who have more laps. Now I want to count the time since start, that is why I don't delete the first time
    query = """
    SELECT pilot_id, 
        COUNT(*) AS total_laps,   -- Total lap number
        SUM(lap_time) AS total_lap_time  -- Sum of lap time
    FROM saved_race_lap
    WHERE deleted = 0
    GROUP BY pilot_id
    ORDER BY total_laps DESC, total_lap_time ASC;
    """

    # Run the query
    c.execute(query)

    # Get fastest pilot
    pilot_id, total_laps, total_time  = c.fetchone()
    c.execute('SELECT callsign FROM pilot WHERE id=?', (pilot_id,))
    pilot_nick = c.fetchone()[0]

    # Save and show result
    pilots_results[pilot_nick].fastest_race += 1
    print(f"Fastest race: Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Total laps: {total_laps} Total time: {total_time/1000.0}s")


    # # Obtain all results
    # results = c.fetchall()

    # # Show all results
    # print("Lap time")
    # for pilot_id, total_laps, total_time in results:
    # #for pilot_id, race_id, lap_time, lap_time_formatted, lap_time_stamp in results:
    #     # Get callsign of pilot
    #     c.execute('SELECT callsign FROM pilot WHERE id=?', (pilot_id,))
    #     pilot_nick = c.fetchone()[0]
    #     print(f"Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Total laps: {total_laps} Total time: {total_time}")
    #     #print(f"Pilot ID: {pilot_id}, Pilot Nick: {pilot_nick}, Formatted lap time: {lap_time_formatted}, Race ID: {race_id}, Fastest lap: {lap_time}, Lap Time Stamp {lap_time_stamp}")
    # print("End lap time")

def compute_fastest_3_consecutive_laps(conn: sqlite3.Connection):
    # Connect to the database
    c = conn.cursor()

    pilot_best_laptimes = {}

    # Get race class
    c.execute('SELECT id, name FROM race_class')
    for raceclass_id, raceclass_name in c.fetchall():
        # Get all heat ids from current race class
        c.execute('SELECT heat_id FROM saved_race_meta WHERE class_id=? ORDER BY heat_id DESC', (raceclass_id, ))

        # For every heat in current raceclass
        for heat_id in c.fetchall():
            current_heat_id = heat_id[0]

            # Get the race id for that heat
            c.execute('SELECT id FROM saved_race_meta WHERE heat_id=?', (current_heat_id, ))
            race_id = c.fetchall()[0][0]

            # Get times of all pilots in that heat
            laptime_pilot = get_fastest_X_consecutive(c, race_id, n_consecutive_laps=3)
            for pilot_nick, best_3_consecutive_laptime in laptime_pilot.items():
                # If the computed time is not none and
                # pilot is not in the list of times or the time is faster than the one we have registred, update
                if best_3_consecutive_laptime is not None and (pilot_nick not in pilot_best_laptimes or pilot_best_laptimes[pilot_nick] > best_3_consecutive_laptime):
                    pilot_best_laptimes[pilot_nick] = best_3_consecutive_laptime

    # Sort from lower to higher times
    sorted_times = dict(sorted(pilot_best_laptimes.items(), key=lambda item: item[1]))

    # Get lower time
    fastest_3_laps_pilot = list(sorted_times.keys())[0]
    fastest_3_laps_time = pilot_best_laptimes[fastest_3_laps_pilot]

    # Increase the number of consecutives 3 fastest laps of that pilot
    pilots_results[fastest_3_laps_pilot].consecutives_3_fastest_laps += 1

    print(f"Fastest 3 consecutive laps {fastest_3_laps_pilot} = {msec_to_min_sec_dec(fastest_3_laps_time)}")


if __name__ == '__main__':
    # Fill pilot points with 2025 pilots
    with open("pilots.txt", "r") as f:
        pilots = f.readlines()
        for pilot_nick in pilots:
            pilot_nick = pilot_nick.splitlines()[0]
            pilots_results[pilot_nick] = Pilot(pilot_nick)

    print(pilots_results)
    print("-------------------------")

    # Read all db and fill pilots structure
    pattern = re.compile(r'.*\.db')
    for database_file in sorted_nicely(os.listdir(IN_FOLDER)):
        # Skip files that do not match the .db pattern
        if not pattern.match(database_file):
            print("Skipping "+database_file)
            continue

        # Connect DB
        databse_file_path = os.path.realpath(os.path.join(IN_FOLDER, database_file))
        print(databse_file_path)
        conn = sqlite3.connect(databse_file_path)
        #print('--- SCHEMA ---')
        #for row in conn.cursor().execute("SELECT name, sql FROM sqlite_master WHERE type='table'"):
        #    print(f"\nTable: {row[0]}\n{row[1]}")

        compute_race_points(conn)
        compute_number_of_laps(conn)
        compute_fastest_lap(conn)
        compute_fastest_race(conn)
        compute_fastest_3_consecutive_laps(conn)
        conn.close()


    
    print("-------------------------")
    print("Current classifications before tiebrakers:")
    sorted_pilot_points = dict(sorted(pilots_results.items(), key=lambda item: item[1].points, reverse=True))

    pilot_object: Pilot
    for pilot_nick, pilot_object in sorted_pilot_points.items():
        print(pilot_nick+" points = "+str(pilot_object.points)+", won races = "+str(pilot_object.won_races)+", consecutive 3 fastest laps = "+str(pilot_object.consecutives_3_fastest_laps)+", position accomulator = "+str(pilot_object.race_position_accomulator)+", completed laps = "+str(pilot_object.completed_laps)+", fastest race = "+str(pilot_object.fastest_race)+", fastest laps = "+str(pilot_object.fastest_laps)+", coin flip:"+str(pilot_object.coin_flip))


    print("-------------------------")
    print("Post tiebrakers")
    pilots_list = list(pilots_results.values())
    pilots_list.sort(key=lambda p: (
    -p.points, # More points is better
    -p.won_races, # More wins is better
    -p.consecutives_3_fastest_laps, # More is better
    p.race_position_accomulator, # Lower average position is better
    -p.completed_laps, # More laps is better
    -p.fastest_race, # More fastest race is better
    -p.fastest_laps, # More fastest laps is better
    p.coin_flip # Fallback coinflip
    # p.nick  # Final alphabetical tiebreaker?
    ))


    classification_csv_file = os.path.realpath(os.path.join(OUT_FOLDER, "classification.csv"))
    with open(classification_csv_file, mode="w") as f:
        writer = csv.writer(f)
        writer.writerow(["nick", "points", "won races", "consecutive 3 fastest laps", "position accomulator", "completed laps", "fastest race", "fastest laps", "coin flip"])
        pilot_object: Pilot
        for pilot_object in pilots_list:
            writer.writerow([pilot_object.nick, pilot_object.points, pilot_object.won_races, pilot_object.consecutives_3_fastest_laps, pilot_object.race_position_accomulator, pilot_object.completed_laps, pilot_object.fastest_race, pilot_object.fastest_laps, pilot_object.coin_flip])
            print(pilot_object.nick+" points = "+str(pilot_object.points)+", won races = "+str(pilot_object.won_races)+", consecutive 3 fastest laps = "+str(pilot_object.consecutives_3_fastest_laps)+", position accomulator = "+str(pilot_object.race_position_accomulator)+", completed laps = "+str(pilot_object.completed_laps)+", fastest race = "+str(pilot_object.fastest_race)+", fastest laps = "+str(pilot_object.fastest_laps)+", coin flip = "+str(pilot_object.coin_flip))

    print("-------------------------")
    pilot_object: Pilot
    for pilot_object in pilots_list:
        print(pilot_object.nick+" = "+str(pilot_object.points)+" points")
