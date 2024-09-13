import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

# global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
MAX_THREADS = 10

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))  # Simula tempo de espera para evitar sobrecarregar o servidor
    try:
        response = requests.get(movie_link, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extração de título, data, classificação, etc.
            try:
                title = soup.find('h1').text.strip()
                date = soup.find('a', {'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color'}).text.strip()
                rating = soup.find('span', {'class': 'sc-7ab21ed2-1 jGRxWM'}).text.strip() if soup.find('span', {'class': 'sc-7ab21ed2-1 jGRxWM'}) else 'N/A'
                plot_text = soup.find('span', {'data-testid': 'plot-xs_to_m'}).text.strip() if soup.find('span', {'data-testid': 'plot-xs_to_m'}) else 'No plot available'

                print(f"Title: {title}, Date: {date}, Rating: {rating}, Plot: {plot_text}")

                with open('movies.csv', mode='a', newline='', encoding='utf-8') as f:
                    movie_writer = csv.writer(f)
                    movie_writer.writerow([title, date, rating, plot_text])

            except Exception as e:
                print(f"Erro ao extrair detalhes do filme: {e}")

        else:
            print(f"Erro ao acessar a página do filme: {response.status_code}")
    
    except requests.RequestException as e:
        print(f"Erro de conexão com {movie_link}: {e}")

def extract_movies(soup):
    try:
        movies_table = soup.find('table', attrs={'data-caller-name': 'chart-moviemeter'}).find('tbody')
        movies_table_rows = movies_table.find_all('tr')
        movie_links = ['https://www.imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

        threads = min(MAX_THREADS, len(movie_links))
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            executor.map(extract_movie_details, movie_links)

    except Exception as e:
        print(f"Erro ao extrair links dos filmes: {e}")

def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    
    try:
        response = requests.get(popular_movies_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Main function to extract the 100 movies from IMDB Most Popular Movies
            extract_movies(soup)
        
        else:
            print(f"Erro ao acessar a página principal: {response.status_code}")
    
    except requests.RequestException as e:
        print(f"Erro de conexão com {popular_movies_url}: {e}")
    
    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()