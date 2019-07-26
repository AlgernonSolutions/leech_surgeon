import re

import bs4
import dateutil


def get_client_attachments(driver, client_id):
    results = []
    attachments = driver.list_client_attachments(client_id)
    soup = bs4.BeautifulSoup(attachments, 'lxml')
    file_bodies = soup.find_all('div', attrs={'name': re.compile(r'divFolder\d')})
    for pointer, file_body in enumerate(file_bodies):
        header_name = f'divFolderHeader{pointer}'
        file_header = soup.find('div', attrs={'name': header_name})
        header_value = [x for x in file_header.stripped_strings][0]
        files = file_body.find_all('tr')
        for file in files:
            date_attached = file.find('td', attrs={'class': 'dateAttachedColumn'}).string
            download_link = file_body.find('a', href=re.compile(r'/common/get_attachment\.aspx\?file_guid='))
            attachment_name = download_link.string
            attachment_link = download_link.attrs['href']
            results.append({
                'attachment_category': header_value,
                'attachment_name': attachment_name,
                'download_link': attachment_link.strip(),
                'date_attached': dateutil.parser.parse(date_attached)
            })
    return results
