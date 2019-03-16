export const CONFIGURATIONS = [
  {
    query : {
      condition: 'and',
      rules: [
        {field: 'age', operator: '<='}
      ]
    },
    config : {
      fields: {
        age: { id: 'org.ga4gh.subject.age', spec : "http://ga4gh.org", name: 'Age', type: 'number'},
        dob : { id: 'org.ga4gh.subject.dateOfBirth', spec: "http://ga4gh.org", name: 'Date of Birth', type: 'date', operators : ['=', '<=', '>'] }
      }
    }
  },
  {
    query : {
      condition: 'and',
      rules: [
      ]
    },
    config : {
      fields: {
        subject_id : {
          "name": "Subject ID",
          "id": "org.ga4gh.subject.id",
          "type": "number",
          "optional": false
        },
        ethnicity: {
          "name": "Ethnicity",
          "type": "string",
          "id": "ca.pgpc.phenotype.ethnicity"
        },
        body_type: {
          "name": "Body type",
          "type": "string",
          "id": "ca.pgpc.phenotype.body_type"
        },
        allergy: {
          "name": "Allergy",
          "type": "string",
          "id": "ca.pgpc.phenotype.allergy"
        },
        weight: {
          "name": "Weight",
          "type": "number",
          "id": "ca.pgpc.phenotype.body_weight",
          "units": "kg"
        },
        medical_procedure: {
          "name": "Medical Procedure",
          "type": "string",
          "id": "ca.pgpc.phenotype.medical_procedure"
        },
        height: {
          "name": "Height",
          "type": "number",
          "id": "ca.pgpc.phenotype.body_height",
          "units": "cm"
        },
        immunization: {
          "name": "Immunization",
          "type": "string",
          "id": "ca.pgpc.phenotype.immunization"
        },
        blood_pressure: {
          "name": "Blood pressure",
          "type": "string",
          "id": "ca.pgpc.phenotype.blood_pressure",
          "units": "mmhg",
          "notes": "Systolic blood pressure, literal \"/\" character, then diastolic blood pressure"
        },
        birth_year: {
          "name": "Birth year",
          "type": "number",
          "id": "ca.pgpc.phenotype.birth_year",
          "precision": "1 year"
        },
        birth_month: {
          "name": "Birth month",
          "type": "number",
          "id": "ca.pgpc.phenotype.birth_month",
          "precision": "1 month"
        },
        birth_year_month: {
          "name": "Birth year month",
          "type": "string",
          "id": "ca.pgpc.phenotype.birth_year_month",
          "precision": "1 month"
        },
        medication: {
          "name": "Medication",
          "type": "string",
          "id": "ca.pgpc.phenotype.medication"
        },
        blood_type: {
          "name": "Blood type",
          "type": "category",
          "id": "ca.pgpc.phenotype.abo_rhd_blood_group",
          "options" : [
            { name : "A+", value: "Group A, RhD positive" },
            { name : "A-", value: "Group A, RhD positive"},
            { name : "AB+", value: "Group AB, RhD positive"},
            { name : "AB-", value: "Group AB, RhD negative"},
            { name : "B+", value: "Group B, RhD positive"},
            { name : "B-", value: "Group B, RhD negative"},
            { name : "O+", value: "Group O, RhD positive"},
            { name : "O-", value: "Group O, RhD negative"}
          ]
        },
        sex: {
          "name": "Sex",
          "type": "category",
          "id": "ca.pgpc.phenotype.sex",
          options: [
            { value: "M", name: "Male" },
            { value: "F", name: "Female" },
            { value: "X", name: "Intersex"}
          ]
        },
        symptom: {
          "name": "Conditions or Symptom",
          "type": "string",
          "id": "ca.pgpc.phenotype.conditions_or_symptom"
        },
        test_result: {
          "name": "Test Result",
          "type": "string",
          "id": "ca.pgpc.phenotype.test_result"
        }
      }
    }
  }
]
