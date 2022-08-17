from time import sleep
import requests
from bs4 import BeautifulSoup


def get_all_information(url):

    all_information_string = []
    try:
        sleep(1)
        starting_url = requests.get(url)
        soup = BeautifulSoup(starting_url.content, "html.parser")

        # Creator information
        concatenate_creator = ""
        creators = soup.find("ul", {
            "class": "ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline "
                     "ipc-metadata-list-item__list-content baseAlt"})
        if creators is None:
            concatenate_creator += "There is no creator"
        else:
            exist_creators = creators.findAll(
                "a",
                {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

            for creator in exist_creators:
                if creator == exist_creators[len(exist_creators) - 1]:
                    concatenate_creator += creator.text
                else:
                    concatenate_creator += creator.text + "|"
        all_information_string.append(concatenate_creator)

        # Stars information
        star_class = soup.find("li", {"class": "ipc-metadata-list__item ipc-metadata-list-item--link"})

        concatenate_stars = ""
        if star_class is None:
            concatenate_stars += "there is no star"
        else:
            stars = star_class.findAll(("a", {
                "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}))
            for star in stars:
                if star == stars[len(stars) - 1]:
                    concatenate_stars += star.text
                else:
                    concatenate_stars += star.text + "|"

        all_information_string.append(concatenate_stars)

        # Details information
        concatenate_company = ""
        details_section = soup.find("div", {"data-testid": "title-details-section"})  # details section
        if details_section is None:
            concatenate_company += "there is no production companies"
        else:
            details_companies = details_section.find("li", {"data-testid": "title-details-companies"})  # companies
            if details_companies is None:
                concatenate_company += "there is no production companies"
            else:
                production_companies = details_companies.findAll("a", {
                    "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

                for production_company in production_companies:
                    if production_company == production_companies[len(production_companies) - 1]:
                        concatenate_company += production_company.text
                    else:
                        concatenate_company += production_company.text + "|"

        all_information_string.append(concatenate_company)

        # Official Sites Details
        concatenate_sites = " "
        if details_section is None:
            concatenate_sites += "There is no official sites"
        else:
            details_class = details_section.find("li", {"data-testid": "details-officialsites"})
            if details_class is None:
                concatenate_sites += "There is no official sites"
            else:
                official_sites = details_class.findAll("a", {
                    "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

                for official_site in official_sites:
                    concatenate_sites += str(official_site.get('href')) + ','
        all_information_string.append(concatenate_sites)
    except Exception as e:
        pass

    return all_information_string

