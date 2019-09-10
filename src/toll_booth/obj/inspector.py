from decimal import Decimal

from algernon import AlgObject


class InspectionFindings(AlgObject):
    def __init__(self, id_source, encounter_id, encounter_datetime_in, provider_id, patient_id, patient_name, findings=None):
        if not findings:
            findings = []
        self._id_source = id_source
        self._encounter_id = encounter_id
        self._encounter_datetime_in = encounter_datetime_in
        self._provider_id = provider_id
        self._patient_id = patient_id
        self._patient_name = patient_name
        self._findings = findings

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['id_source'], json_dict['encounter_id'], json_dict['provider_id'],
            json_dict['patient_id'], json_dict['patient_name'],  json_dict.get('findings'))

    @property
    def id_source(self):
        return self._id_source

    @property
    def encounter_id(self):
        return self._encounter_id

    @property
    def encounter_datetime_in(self):
        return self._encounter_datetime_in

    @property
    def provider_id(self):
        return self._provider_id

    @property
    def patient_id(self):
        return self._patient_id

    @property
    def patient_name(self):
        return self._patient_name

    @property
    def findings(self):
        return self._findings


class InspectionFinding(AlgObject):
    def __init__(self, finding_name, finding_message, finding_details=None):
        if not finding_details:
            finding_details = {}
        self._finding_name = finding_name
        self._finding_message = finding_message
        self._finding_details = finding_details

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['finding_name'], json_dict['finding_message'],  json_dict.get('finding_details'))

    @property
    def finding_name(self):
        return self._finding_name

    @property
    def finding_message(self):
        return self._finding_message

    @property
    def finding_details(self):
        return self._finding_details

    def __str__(self):
        def _format_details(details):
            formatted = []
            for key, value in details.items():
                formatted.append(f'{str(key)}: {str(value)}')
            if formatted:
                return f" [{', '.join(formatted)}]"
            return ''
        return f'{self._finding_message}{_format_details(self.finding_details)}'


class InspectionEncounterData(AlgObject):
    def __init__(self, encounters, encounters_by_date, encounters_by_provider, encounters_by_patient):
        self._encounters = encounters
        self._encounters_by_date = encounters_by_date
        self._encounters_by_provider = encounters_by_provider
        self._encounters_by_patient = encounters_by_patient

    @classmethod
    def from_raw_encounters(cls, raw_encounters):
        by_date = {}
        by_provider = {}
        by_patient = {}
        encounters = {}
        for encounter in raw_encounters:
            encounter_id = encounter['Service ID']
            provider_id = encounter['Staff ID']
            patient_id = encounter['Consumer ID']
            encounter_date = encounter['Service Date'].isoformat()
            if provider_id not in by_provider:
                by_provider[provider_id] = []
            by_provider[provider_id].append(encounter)
            if patient_id not in by_patient:
                by_patient[patient_id] = []
            by_patient[patient_id].append(encounter)
            if encounter_date not in by_date:
                by_date[encounter_date] = []
            by_date[encounter_date].append(encounter)
            encounters[encounter_id] = encounter
        return cls(encounters, by_date, by_provider, by_patient)

    @classmethod
    def parse_json(cls, json_dict):
        encounters = {Decimal(x): y for x, y in json_dict['encounters'].items()}
        return cls(
            encounters, json_dict['encounters_by_date'],
            json_dict['encounters_by_provider'], json_dict['encounters_by_patient'])

    @property
    def to_json(self):
        encounters = {float(x): y for x, y in self._encounters.items()}
        return {
            '_encounters': encounters,
            '_encounters_by_date': self._encounters_by_date,
            '_encounters_by_provider': self._encounters_by_provider,
            '_encounters_by_patient': self._encounters_by_patient,
        }

    @property
    def encounters_by_date(self):
        return self._encounters_by_date

    @property
    def encounters_by_provider(self):
        return self._encounters_by_provider

    @property
    def encounters_by_patient(self):
        return self._encounters_by_patient

    def get_same_day_encounters_by_id(self, encounter_id):
        if encounter_id not in self._encounters:
            raise KeyError(encounter_id)
        encounter_date = self._encounters[encounter_id]['Service Date'].isoformat()
        return self.get_same_day_encounters_by_date(encounter_date)

    def get_same_day_encounters_by_date(self, encounter_date):
        encounter_date = encounter_date.isoformat()
        if encounter_date in self._encounters_by_date:
            return self._encounters_by_date[encounter_date]
        encounter_date = encounter_date.replace('+00:00', '')
        if encounter_date in self._encounters_by_date:
            return self._encounters_by_date[encounter_date]
        raise KeyError(encounter_date)

    def __iter__(self):
        return iter(self._encounters.values())
