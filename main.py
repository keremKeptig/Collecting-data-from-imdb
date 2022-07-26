import requests
from bs4 import BeautifulSoup

def get_all_information(url):
    starting_url = requests.get(url)
    content = starting_url.content

    soup = BeautifulSoup(content, "html.parser")#to make parse

    creator = soup.find("a", {"class": "ipc-metadata-list-item__list-content-item "
                                       "ipc-metadata-list-item__list-content-item--link"})
    print("Creator: " + creator.text)

    #to access details inside the box
    star_class = soup.find("li", {"class": "ipc-metadata-list__item ipc-metadata-list-item--link"})
    #to get all the authors inside the box
    stars = star_class.findAll("a", {
        "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

    concatenate_stars = "Stars: "
    for star in stars:
        if star == stars[len(stars)-1]:
            concatenate_stars += star.text
        else:
            concatenate_stars += star.text + ","

    print(concatenate_stars)

    #Details information
    details_section = soup.find("div", {"data-testid": "title-details-section"})  # details section
    details_companies = details_section.find("li", {"data-testid": "title-details-companies"})  # companies
    production_companies = details_companies.findAll("a", {
        "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

    concatenate_company = "Production companies: "
    for production_company in production_companies:
        if production_company == production_companies[len(production_companies)-1]:
            concatenate_company += production_company.text
        else:
            concatenate_company += production_company.text + ","
    print(concatenate_company)

    #official Sites Details
    official_sites_details = details_section.find("li", {"data-testid": "details-officialsites"})
    oficial_sites = official_sites_details.findAll("a", {
        "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})
    print("Official sites:")
    for oficialSite in oficial_sites:
        print(oficialSite.get('href'))

    #More Like This
    moreLikeThis = soup.find("section", {"data-testid": "MoreLikeThis"})
    titleClass = soup.findAll("div", {
        "class": "ipc-poster-card ipc-poster-card--base ipc-poster-card--dynamic-width ipc-sub-grid-item ipc-sub-grid-item--span-2"})

    print("More like this:")
    for i in range(len(titleClass)):
        title = titleClass[i].find("span", {"data-testid": "title"})
        print(title.text)

url = "https://www.imdb.com/title/tt1190634/"
get_all_information(url)


