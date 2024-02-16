import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_data(url):
    response = requests.get(url)

    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        data_list = []

        # Find all the data download sections
        data_sections = soup.find_all('h3')

        for section in data_sections:
            # Extract city name and date
            city_name = section.text.strip()
            date_with_explore = section.find_next('h4').text.strip()

            # Remove "(Explore)" part from the date
            date = date_with_explore.replace('(Explore)', '').strip()

            # Extract state/region and country from city name
            city_parts = city_name.split(', ')
            city = city_parts[0]
            state_region = city_parts[1] if len(city_parts) > 1 else None
            country = city_parts[2] if len(city_parts) > 2 else None

            # Extract links from the corresponding table
            links_table = section.find_next('table')
            links = links_table.find_all('a', href=True)

            # Create a list of dictionaries to store the data
            for link in links:
                file_name = link.text.strip()
                file_url = link['href']

                data_list.append({
                    'city': city,
                    'state': state_region,
                    'country': country,
                    'date': date,
                    'file_name': file_name,
                    'file_url': file_url
                })

        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(data_list)

        # Format the 'Date' column
        df['formatted_date'] = pd.to_datetime(df['date'], format='%d %B, %Y', errors='coerce')

        # Return the DataFrame
        return df

    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None

# # Example usage:
# url_to_scrape = 'your_target_url_here'
# result_df = scrape_data(url_to_scrape)

# if result_df is not None:
#     # Display the head of the DataFrame
#     print(result_df.head())