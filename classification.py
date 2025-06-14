import sqlite3
import os
import random

RANK_POINTS = [25,20,18,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,1,1,1,1,1,1,1,1,1,1]
pilots_results = {}
IN_FOLDER = "in_databases"
PRINT_HEAT_RESULTS = True

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

def get_sorted_heat_results(cursor:sqlite3.Cursor, race_id, heat_id):
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
    # TODO: Finish filling pilot class
    for database_file in os.listdir(IN_FOLDER):
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
        conn.close()


    
    print("-------------------------")
    print("Current classifications before tiebrakers:")
    sorted_pilot_points = dict(sorted(pilots_results.items(), key=lambda item: item[1].points, reverse=True))

    pilot_object: Pilot
    for pilot_nick, pilot_object in sorted_pilot_points.items():
        print(pilot_nick+" points = "+str(pilot_object.points)+", won races = "+str(pilot_object.won_races)+", consecutive 3 fastest laps = "+str(pilot_object.consecutives_3_fastest_laps)+", position accomulator = "+str(pilot_object.race_position_accomulator)+", completed laps = "+str(pilot_object.completed_laps)+", fastest race = "+str(pilot_object.fastest_race)+", fastest laps = "+str(pilot_object.fastest_laps))


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
    pilot_object: Pilot
    for pilot_object in pilots_list:
        print(pilot_object.nick+" points = "+str(pilot_object.points)+", won races = "+str(pilot_object.won_races)+", consecutive 3 fastest laps = "+str(pilot_object.consecutives_3_fastest_laps)+", position accomulator = "+str(pilot_object.race_position_accomulator)+", completed laps = "+str(pilot_object.completed_laps)+", fastest race = "+str(pilot_object.fastest_race)+", fastest laps = "+str(pilot_object.fastest_laps))
