import { FormBuilder, FormControl } from '@angular/forms';
import { Component, Inject, OnInit } from '@angular/core';
import { ApiService } from './app.api.service';
import { Field } from './model/search/field';
import { Rule, RuleSet } from 'angular2-query-builder';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material';
import { JsonDialog } from './dialog/json/json-dialog';
import { FieldsDialog } from './dialog/fields/fields-dialog';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent implements OnInit {
  public queryCtrl: FormControl;

  events: string[] = [];
  opened: boolean;

  public query = {
    condition: 'and',
    rules: [
      {
        "field": "demo_view.chromosome",
        "operator": "=",
        "value": "chr1"
      },
      {
        "field": "demo_view.start_position",
        "operator": "=",
        "value": 5087263
      },
      {
        "field": "demo_view.reference_base",
        "operator": "=",
        "value": "A"
      },
      {
        "field": "demo_view.alternate_base",
        "operator": "=",
        "value": "G"
      }
    ],
    limit : 100,
    offset : 0
  };

  public config = {
    fields: undefined
  };

  public view = {
    wrapResultTableCells: true,
    isQuerying: false,
    selectedTabIndex: 0,
    queryChanged : false
  }

  public results = null;

  private jsonDialogRef: MatDialogRef<JsonDialog>;
  private fieldsDialogRef: MatDialogRef<FieldsDialog>;

  constructor(
    private formBuilder: FormBuilder,
    private apiService: ApiService,
    private dialog: MatDialog
  ) {
    this.queryCtrl = this.formBuilder.control(this.query);
  }

  transformRule(rule: RuleSet | Rule) {
    if ('condition' in rule) {
      return this.transformCondition(rule)
    } else if ('operator' in rule) {
      return this.transformOperator(rule)
    } else {
      throw new Error('Unknown rule format');
    }
  }

  transformCondition(rule: RuleSet) {
    var predicates = []
    for (let subrule of rule.rules) {
      predicates.push(this.transformRule(subrule))
    }
    return {
      'p': rule.condition,
      'predicates': predicates
    }
  }

  transformOperator(rule: Rule) {
    var p = {
      'p': rule.operator,
      'rfield': rule.field
    }
    p[this.valueKey(rule.field)] = rule.value
    return p
  }

  valueKey(fieldName: string): string {
    var field: Field = this.config.fields[fieldName]
    if (field.type == 'string') {
      return 'lstring'
    } else if (field.type == 'number') {
      return 'lnumber'
    } else if (field.type == 'boolean') {
      return 'lboolean'
    } else {
      return 'lvalue'
    }
  }

  paginationChanged(event) {
    this.query.limit = event.pageSize;
    this.query.offset = event.pageSize * event.pageIndex;
    this.view.queryChanged = true;
    /*this.doQuery(this.query);*/
  }

  transformQuery(query: RuleSet) {
    // we're hardcoding select clause here for demo purposes
    // the flexible thing is the rule built by query builder
    return {
      'select': [
        {
          "field": "participant_id"
        },
        {
          "field": "chromosome"
        },
        {
          "field": "start_position"
        },
        {
          "field": "reference_base"
        },
        {
          "field": "alternate_base"
        },
        {
          "field": "vcf_size"
        },
        {
          "field": "vcf_urls"
        },
        {
          "field": "category"
        },
        {
          "field": "key"
        },
        {
          "field": "raw_value",
          "alias": "value"
        }],
      'from': [{
        'table': 'demo_view'
      }],
      'where': this.transformRule(query),
      'limit': 10
      /*,'offset': query.offset*/ // trying to get offsets to work
    }
  }

  public showJson(): void {
    this.jsonDialogRef = this.dialog.open(JsonDialog, {
      width: '90%',
      height: '90%',
      data: { query: this.query }
    });
  }

  public showFields(): void {
    this.fieldsDialogRef = this.dialog.open(FieldsDialog, {
      width: '90%',
      height: '90%',
      data: { fields: this.config.fields }
    });
  }

  public doQuery(query) {
    this.view.isQuerying = true;
    console.log("Original query\n" + JSON.stringify(query, null, 2))
    var transformedQuery = this.transformQuery(query)
    console.log("Transformed query\n" + JSON.stringify(transformedQuery, null, 2))
    this.apiService.doQuery(transformedQuery).subscribe(
      (dto) => {
        console.log(dto);
        this.view.queryChanged = false;
        this.results = dto;
        this.view.isQuerying = false;
        this.view.selectedTabIndex = 2;
      },
      (err) => {
        console.log('Error', err)
        this.view.isQuerying = false;
      });
  }

  queryChanged($event) {
    this.view.queryChanged = true;
  }

  normalizeArray<T>(array: Array<T>, indexKey: keyof T) {
    const normalizedObject: any = {}
    for (let i = 0; i < array.length; i++) {
      const key = array[i][indexKey]
      normalizedObject[key] = array[i]
    }
    return normalizedObject as { [key: string]: T }
  }


  ngOnInit(): void {

    this.apiService
      .getFields()
      .subscribe((fields: Field[]) => {
        this.config.fields = this.normalizeArray(fields, 'id');
        this.view.selectedTabIndex = 0;
      },
        (err) => console.log('Error', err));
  }
}
