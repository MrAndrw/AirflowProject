company_names_en = ["Google", "Apple", "Microsoft", "Amazon", "Tesla"]
company_names_ru = ["Газпром", "Сбер", "СТЦ"]
company_names_ukr = ["Завод"]
company_names_zh = ["长安"]
country_names = ["USA", "Canada", "Germany", "France", "Russia"]  # Список стран можно дополнить
positions_en = ["Software Engineer", "Product Manager", "Data Scientist", "CEO",
                "Marketing Specialist"]  # Дополнить список профессий
positions_ru = ["Системный аналитик"]
positions_ukr = ["Доктор"]
positions_zh = ["软件工程师", "产品经理", "数据科学家", "CEO", "市场专家"]

head_names = ["firstname, FirstName", "first", "First Name", "first name", "Firstname", "name",
              "lastname", "LastName", "last", "Last Name", "last name", "Lastname",
              "middlename", "MiddleName", "Middle Name", "Middle name", "middle", "middle name",
              "fullname", "Fullname", "FullName", "Full Name", "full name", 'Bill', 'bill',
              "phone", "Phone", "mobile", "Mobile", "Phone number", "phone number", "PhoneNum", "Phone_pfx",
              "Phone10", "Phone Num", "Phone Number", "login", "Login", "Email", "email", "mail", "password",
              "Password", "pass", "Pass", "hash", "Hash", "zip", "Address", "address", "street", "Street", "House",
              "house", "city", "City", "State", "state", "region", "Region", "Country", "country", "area", "Area",
              "district", "District", "id_number", "IdNum", "Id_Number", "Id_number", "Id_num", "id_num", "personal_id",
              "Personal_Id", "Personal_id", "PersonalId", "long", "longitude", "Long", "Longitude", "Lat", "lat",
              "Latitude", "latitude", 'Listing ID', 'Processed Date', 'Status', 'Source', 'Source URL', 'Address',
              'City', 'State', 'Zip', 'County', 'Price', 'Bedrooms', 'Bathrooms', 'Year Built',
              'Property type', 'Square Footage', 'Lot Size', 'Contact Phone', 'Contact Phone DNC',
              'Contact Line Type', 'List Agent Name',
              'List Agent Phone', 'List Agent Email', 'Listing Office Name', 'MLS Number', 'Owner Name1',
              'Owner Name2', 'Owner First Name', 'Owner Last Name',
              'Owner Address', 'Owner City', 'Owner State', 'Owner Zip', 'Owner Occupied',
              'Tax Beds', 'Tax Baths', 'Tax Year', 'Tax Square Footage', 'Tax Lot', 'Taxes',
              'Tax Assessed Value', 'Sale Amount', 'Sale Date', 'APN', 'FIPS', 'User Phone 1',
              'User Phone 2', 'Augmented Name 1', 'Augmented Phone 1_1',
              'Augmented Phone 1_1 DNC', 'Augmented Line Type 1_1', 'Augmented Phone 1_2',
              'Augmented Phone 1_2 DNC', 'Augmented Line Type 1_2', 'Augmented Email 1_1',
              'Augmented Email 1_2', 'Augmented Email 1_3', 'Augmented Name 2', 'Augmented Phone 2_1',
              'Augmented Phone 2_1 DNC', 'patronymic',
              'Augmented Line Type 2_1', 'Augmented Phone 2_2', 'Augmented Phone 2_2 DNC',
              'Augmented Line Type 2_2', 'Augmented Email 2_1', 'Augmented Email 2_2',
              'Augmented Email 2_3', 'Augmented Name 3', 'Augmented Phone 3_1', 'Augmented Phone 3_1 DNC',
              'Augmented Line Type 3_1', 'Augmented Phone 3_2',
              'Augmented Phone 3_2 DNC', 'Augmented Line Type 3_2', 'Augmented Email 3_1',
              'Augmented Email 3_2', 'Augmented Email 3_3', 'Augmented Name 4', 'Augmented Phone 4_1',
              'Augmented Phone 4_1 DNC', 'Augmented Line Type 4_1', 'Augmented Phone 4_2',
              'Augmented Phone 4_2 DNC', 'Augmented Line Type 4_2', 'Augmented Email 4_1',
              'Augmented Email 4_2', 'Augmented Email 4_3', 'Remarks/Ad Text', 'Disposition',
              'Contact Notes', 'Subdivision', 'Basic Name1', 'Basic Phone1', 'Basic Name2',
              'Basic Phone2', 'Estimated Value', 'Foreclosure Instrument Number',
              'Foreclosure Recording Date', 'Foreclosure Instrument Date', 'Foreclosure Case Number',
              'Loan Recording Date', 'Loan Instrument Number', 'Borrower Name', 'Loan Number',
              'Interest Rate', 'Lender Name', 'Lender Street Address', 'Lender City', 'Lender State',
              'Lender Zip', 'Servicer Name', 'Servicer Address', 'Servicer City', 'Servicer State',
              'Servicer Zip', 'Servicer Phone', 'Trustee Name',
              'Trustee Address', 'Trustee City', 'Trustee State', 'Trustee Zip', 'Trustee Phone',
              'Auction Date', 'Auction Address', 'Default Amount'
]

country_mapping = {
    'ab': ['Abkhazia'],
    'af': ['Afghanistan'],
    'al': ['Albania'],
    'dz': ['Algeria'],
    'as': ['American Samoa (US)'],
    'ad': ['Andorra'],
    'ao': ['Angola'],
    'ai': ['Anguilla'],
    'ag': ['Antigua and Barbuda'],
    'ar': ['Argentina'],
    'am': ['Armenia'],
    'aw': ['Aruba (Netherlands)'],
    'au': ['Australia'],
    'at': ['Austria'],
    'az': ['Azerbaijan'],
    'bs': ['Bahamas'],
    'bh': ['Bahrain'],
    'bd': ['Bangladesh'],
    'bb': ['Barbados'],
    'by': ['Belarus'],
    'be': ['Belgium'],
    'bz': ['Belize'],
    'bj': ['Benin'],
    'bm': ['Bermuda'],
    'bt': ['Bhutan'],
    'bo': ['Bolivia'],
    'ba': ['Bosnia and Herzegovina'],
    'bw': ['Botswana'],
    'br': ['Brazil'],
    'io': ['British Indian Ocean Territory'],
    'vg': ['British Virgin Islands'],
    'bn': ['Brunei'],
    'bg': ['Bulgaria'],
    'bf': ['Burkina Faso'],
    'bi': ['Burundi'],
    'kh': ['Cambodia'],
    'cm': ['Cameroon'],
    'ca': ['Canada'],
    'cv': ['Cape Verde'],
    'ky': ['Cayman Islands'],
    'cf': ['Central African Republic'],
    'td': ['Chad'],
    'cl': ['Chile'],
    'cn': ['China'],
    'co': ['Colombia'],
    'km': ['Comoros'],
    'cg': ['Congo'],
    'ck': ['Cook Islands'],
    'cr': ['Costa Rica'],
    'hr': ['Croatia'],
    'cu': ['Cuba'],
    'an': ['Curacao (Netherlands)', 'Curacao'],
    'cw': ['Curacao (Netherlands)', 'Curacao'],
    'cy': ['Cyprus'],
    'cz': ['Czech Republic'],
    'cd': ['Democratic Republic of the Congo'],
    'dk': ['Denmark'],
    'dg': ['Diego Garcia'],
    'dj': ['Djibouti'],
    'dm': ['Dominica'],
    'do': ['Dominican Republic'],
    'tl': ['East Timor'],
    'ec': ['Ecuador'],
    'eg': ['Egypt'],
    'sv': ['El Salvador'],
    'gq': ['Equatorial Guinea'],
    'er': ['Eritrea'],
    'ee': ['Estonia'],
    'et': ['Ethiopia'],
    'fk': ['Falkland Islands'],
    'fo': ['Faroe Islands'],
    'fm': ['Federated States of Micronesia'],
    'fj': ['Fiji'],
    'fi': ['Finland'],
    'fr': ['France'],
    'bl': ['Saint Barthélemy'],
    'gf': ['French Guiana'],
    'mf': ['Saint Martin (French Part)'],
    'mq': ['Martinique'],
    'pf': ['French Polynesia (France)'],
    'ga': ['Gabon'],
    'gm': ['Gambia'],
    'ge': ['Georgia'],
    'de': ['Germany'],
    'gh': ['Ghana'],
    'gi': ['Gibraltar'],
    'gr': ['Greece'],
    'gl': ['Greenland'],
    'gd': ['Grenada'],
    'gp': ['Guadeloupe (France)'],
    'gu': ['Guam (USA)'],
    'gt': ['Guatemala'],
    'gn': ['Guinea'],
    'gw': ['Guinea-Bissau'],
    'gy': ['Guyana'],
    'ht': ['Haiti'],
    'hn': ['Honduras'],
    'hk': ['Hong Kong'],
    'hu': ['Hungary'],
    'is': ['Iceland'],
    'in': ['India'],
    'id': ['Indonesia'],
    'ir': ['Iran'],
    'iq': ['Iraq'],
    'ie': ['Ireland'],
    'im': ['Isle of Man'],
    'il': ['Israel'],
    'it': ['Italy'],
    'ci': ['Ivory Coast'],
    'jm': ['Jamaica'],
    'jp': ['Japan'],
    'jo': ['Jordan'],
    'kz': ['Kazakhstan'],
    'ke': ['Kenya'],
    'ki': ['Kiribati'],
    'xk': ['Kosovo'],
    'kw': ['Kuwait'],
    'kg': ['Kyrgyzstan'],
    'la': ['Laos'],
    'lv': ['Latvia'],
    'lb': ['Lebanon'],
    'ls': ['Lesotho'],
    'lr': ['Liberia'],
    'ly': ['Libya'],
    'li': ['Liechtenstein'],
    'lt': ['Lithuania'],
    'lu': ['Luxembourg'],
    'mo': ['Macau (PRC)'],
    'mg': ['Madagascar'],
    'mw': ['Malawi'],
    'my': ['Malaysia'],
    'mv': ['Maldives'],
    'ml': ['Mali'],
    'mt': ['Malta'],
    'mh': ['Marshall Islands'],
    'mr': ['Mauritania'],
    'mu': ['Mauritius'],
    'mx': ['Mexico'],
    'md': ['Moldova'],
    'mc': ['Monaco'],
    'mn': ['Mongolia'],
    'me': ['Montenegro'],
    'ms': ['Montserrat'],
    'ma': ['Morocco'],
    'mz': ['Mozambique'],
    'mm': ['Myanmar'],
    'na': ['Namibia'],
    'nr': ['Nauru'],
    'np': ['Nepal'],
    'nl': ['Netherlands'],
    'nc': ['New Caledonia (France)'],
    'nz': ['New Zealand'],
    'ni': ['Nicaragua'],
    'ne': ['Niger'],
    'ng': ['Nigeria'],
    'nu': ['Niue'],
    'kp': ['North Korea'],
    'mk': ['North Macedonia'],
    'mp': ['Northern Mariana Islands'],
    'no': ['Norway'],
    'om': ['Oman'],
    'pk': ['Pakistan'],
    'pw': ['Palau'],
    'ps': ['Palestinian Territories'],
    'pa': ['Panama'],
    'pg': ['Papua New Guinea'],
    'py': ['Paraguay'],
    'pe': ['Peru'],
    'ph': ['Philippines'],
    'pl': ['Poland'],
    'pt': ['Portugal'],
    'pr': ['Puerto Rico (US)'],
    'qa': ['Qatar'],
    'yt': ['Mayotte'],
    're': ['Reunion'],
    'ro': ['Romania'],
    'ru': ['Russian Federation'],
    'rw': ['Rwanda'],
    'sh': ['Saint Helena and Ascension'],
    'kn': ['Saint Kitts and Nevis'],
    'lc': ['Saint Lucia'],
    'pm': ['Saint Pierre and Miquelon (France)'],
    'vc': ['Saint Vincent and the Grenadines'],
    'ws': ['Samoa'],
    'sm': ['San Marino'],
    'st': ['Sao Tome and Principe'],
    'xx': ['Satellite Networks'],
    'sa': ['Saudi Arabia'],
    'sn': ['Senegal'],
    'rs': ['Serbia'],
    'sc': ['Seychelles'],
    'sl': ['Sierra Leone'],
    'sg': ['Singapore'],
    'sk': ['Slovakia'],
    'si': ['Slovenia'],
    'sb': ['Solomon Islands'],
    'so': ['Somalia'],
    'za': ['South Africa'],
    'kr': ['South Korea'],
    'ss': ['South Sudan'],
    'es': ['Spain'],
    'lk': ['Sri Lanka'],
    'sd': ['Sudan'],
    'sr': ['Suriname'],
    'sz': ['Swaziland'],
    'se': ['Sweden'],
    'ch': ['Switzerland'],
    'sy': ['Syria'],
    'tw': ['Taiwan'],
    'tj': ['Tajikistan'],
    'tz': ['Tanzania'],
    'th': ['Thailand'],
    'tg': ['Togolese Republic'],
    'tk': ['Tokelau'],
    'to': ['Tonga'],
    'tt': ['Trinidad and Tobago'],
    'tn': ['Tunisia'],
    'tr': ['Turkey'],
    'tm': ['Turkmenistan'],
    'tc': ['Turks and Caicos Islands'],
    'tv': ['Tuvalu'],
    'ug': ['Uganda'],
    'ua': ['Ukraine'],
    'ae': ['United Arab Emirates'],
    'gb': ['United Kingdom'],
    'us': ['United States of America', 'US', 'USA'],
    'uy': ['Uruguay'],
    'uz': ['Uzbekistan'],
    'vu': ['Vanuatu'],
    'va': ['Vatican'],
    've': ['Venezuela'],
    'vn': ['Vietnam'],
    'wf': ['Wallis and Futuna'],
    'ye': ['Yemen'],
    'zm': ['Zambia'],
    'zw': ['Zimbabwe'],
}
checkwords = ['by', 'up', 'as', 'at', 'bd', 'io', 'do', 'eg', 'ee', 'is', 'my', 'py', 'so', 'no', 'xx']