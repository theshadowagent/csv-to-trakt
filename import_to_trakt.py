import requests
import time
import pickle
import pandas as pd
import math
import json
import webbrowser
from tqdm import tqdm

CLIENT_ID = 'd724541a15760346c6b2906b74d2aea2804263e384c84f792d1488e25288a303'
CLIENT_SECRET = '6fd38c30fca475a01e26e8c04489522ebdda318287132e909c64971d5a24e57f'
HEADERS = {
    'Content-Type': 'application/json',
    'trakt-api-version': '2',
    'trakt-api-key': CLIENT_ID
}


class TraktImporter:
    def __init__(self):
        self.session = None

    def pickle_token(self, access_token):
        with open('access_token.pickle', 'wb') as f:
            pickle.dump(access_token, f)

    def unpickle_token(self):
        try:
            with open('access_token.pickle', 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return None

    def save_not_found(self, not_found_movies, is_movies):
      filename = 'movies' if is_movies else 'episodes'
      with open(f'not_found/{filename}.csv', 'w') as output:
          json.dump(not_found_movies, output)

    def get_trakt_session(self):
        session = requests.session()
        session.headers.update(HEADERS)
        access_token = self.unpickle_token()

        if not access_token:
            resp = requests.post('https://api.trakt.tv/oauth/device/code',
                                 json={
                                     'client_id': CLIENT_ID
                                 }).json()

            print()
            print(f"To connect with your Trakt open this link: {resp['verification_url']}")
            print()
            print(
                f"Enter {resp['user_code']} to authorize the import"
            )
            print(
                "(you can revoke the access anytime on https://trakt.tv/oauth/authorized_applications)"
            )
            print()
            # time.sleep(2)
            # webbrowser.open(resp['verification_url'])

            expires_in = resp['expires_in']
            interval = resp['interval']
            device_code = resp['device_code']
            access_token = None
            for i in range(int(expires_in / interval)):
                r = requests.post('https://api.trakt.tv/oauth/device/token',
                                  data={
                                      'client_id': CLIENT_ID,
                                      'client_secret': CLIENT_SECRET,
                                      'code': device_code,
                                  })
                if r.status_code == 200:
                    r = r.json()
                    access_token = r['access_token']
                    self.pickle_token(access_token)
                    break
                else:
                    time.sleep(interval)

        session.headers.update({'Authorization': f'Bearer {access_token}'})
        return session

    def import_episodes(self):
        shows_df = pd.read_csv('data/Bibliography - TV Shows.csv')
        shows = []
        show_names = shows_df['Name'].unique()
        for name in show_names:
            show = {'title': name, 'seasons': []}
            seasons = shows_df[shows_df['Name'] == name]['Season'].unique()
            for season in seasons:
                try:
                    episodes_rows = shows_df.loc[shows_df['Name'] == name].loc[
                        shows_df['Season'] == season]
                    show['seasons'].append({
                        'number': int(season),
                        'episodes': [{
                            'watched_at': row['Date Watched'],
                            'number': row['Episode']
                        } for _, row in episodes_rows.iterrows()]
                    })
                except ValueError:
                    continue
            shows.append(show)
        r = self.session.post('https://api.trakt.tv/sync/history',
                              json={
                                  'shows': shows
                              }).json()
        print(f"Imported {r['added']['episodes']} / {shows_df.shape[0]} episodes")
        print(f"Not found {len(r['not_found']['shows'])} / {shows_df.shape[0]} episodes")

        self.save_not_found(r['not_found']['shows'], is_movies=False)

    def import_movies(self):
        movies_df = pd.read_csv('data/Bibliography - Movies.csv')
        movies = []
        for index, row in movies_df.iterrows():
            ids = {}
            movie = {
                'watched_at': f'{row["Date Began"]}T00:30:00.000Z',
                'title': row['Movie Title'],
                'ids': ids
            }
            if type(row['IMDB Link']) == str:
                ids['imdb'] = row['IMDB Link'].split("/")[-2]
            if not math.isnan(row['Release Date']):
                movie['year'] = int(row['Release Date'])
            movies.append(movie)
        r = self.session.post('https://api.trakt.tv/sync/history',
                              json={
                                  'movies': movies
                              }).json()
        print(f"Imported {r['added']['movies']} / {len(movies)} movies")

        self.fix_not_found_movies(movies_df, r)

    def fix_not_found_movies(self, movies_df, trakt_response):
        # For not found movies Trakt returns only watched dates
        nf_watched_dates = list(
            map(lambda x: x['watched_at'].split('T')[0],
                trakt_response['not_found']['movies']))
        nf_movies_df = movies_df[movies_df['Date Began'].isin(
            nf_watched_dates)]
        nf_titles = nf_movies_df['Movie Title'].values
        trakt_id_map = self.find_movies_on_trakt(
            nf_titles, nf_movies_df['Release Date'].values)

        nf_movies = []
        failed_to_find = []
        for nf_title in nf_titles:
            row = nf_movies_df[nf_movies_df['Movie Title'] == nf_title]
            if nf_title not in trakt_id_map:
                failed_to_find.append(nf_title)
                continue
            movie = {
                'watched_at': f'{row["Date Began"].values[0]}T00:30:00.000Z',
                'title': nf_title,
                'ids': {
                    'trakt': trakt_id_map[nf_title]
                }
            }
            if not math.isnan(row['Release Date'].values[0]):
                movie['year'] = int(row['Release Date'].values[0])
            nf_movies.append(movie)

        r = self.session.post('https://api.trakt.tv/sync/history',
                              json={
                                  'movies': nf_movies
                              }).json()
        print(f"Re-imported {r['added']['movies']} / {len(nf_movies)} more movies")
        
        self.save_not_found(failed_to_find, is_movies=True)

    def find_movies_on_trakt(self, not_found_movies, release_years):
        trakt_ids_map = {}
        for idx, nf_movie in tqdm(enumerate(not_found_movies)):
            res = self.session.get(
                f'https://api.trakt.tv/search/movie?query={nf_movie}').json()
            res = list(
                filter(
                    lambda x: not release_years[idx] or x['movie']['year'] and
                    abs(x['movie']['year'] - release_years[idx]) < 2, res))
            if not release_years[idx] and len(res) == 1:
                trakt_ids_map[nf_movie] = res[0]['movie']['ids']['trakt']
            if release_years[idx] and len(res):
                trakt_ids_map[nf_movie] = res[0]['movie']['ids']['trakt']
        return trakt_ids_map

    def run(self):
        self.session = self.get_trakt_session()

        try:
          self.import_movies()
        except BaseException as e:
          print("Error importing movies")
          print(e)

        try:
          self.import_episodes()
        except BaseException as e:
          print("Error importing episodes")
          print(e)


if __name__ == "__main__":
    trakt = TraktImporter()
    trakt.run()
