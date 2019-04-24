import { FormBuilder, FormControl } from '@angular/forms';
import { Component, Inject, OnInit } from '@angular/core';
import { ApiService } from './app.api.service';
import { Field } from './model/search/field';
import { Rule, RuleSet } from 'angular2-query-builder';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material';
import { JsonDialog } from './dialog/json/json-dialog';
import { FieldsDialogComponent } from './dialog/fields/fields-dialog.component';
import { AppConfigService } from './app-config.service';

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
    select: [
      {
        "field": "participant_id"
      },
      {
        "field": "category"
      },
      {
        "field": "key"
      },
      {
        "field": "raw_value"
      }],
    from: 'demo_view',
    where: {
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
      ]
    },
    limit: 100,
    offset: 0
  };

  public config = {
    fields: undefined
  };

  public view = {
    showJSONs: false,
    sidebarOpened: false,
    wrapResultTableCells: true,
    isQuerying: false,
    selectedTabIndex: 0,
    queryChanged: false,
    displayQueryComponents : {
      select : true,
      from : true,
      where : true,
      limit : true,
      offset : true
    }
  }
  

  public results = null;

  private jsonDialogRef: MatDialogRef<JsonDialog>;
  private fieldsDialogRef: MatDialogRef<FieldsDialogComponent>;

  constructor(
    private app: AppConfigService,
    private formBuilder: FormBuilder,
    private apiService: ApiService,
    private dialog: MatDialog
  ) {
    this.view.showJSONs = app.config.developerMode;
    this.queryCtrl = this.formBuilder.control(this.query.where);
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

  jsonify(str: string) {
    console.log(str);
    return JSON.parse(JSON.stringify(str));
  }

  paginationChanged(event) {
    this.query.limit = event.pageSize;
    this.query.offset = event.pageSize * event.pageIndex;
    this.view.queryChanged = true;
    /*this.doQuery(this.query);*/
  }

  transformQuery() {
    // we're hardcoding select clause here for demo purposes
    // the flexible thing is the rule built by query builder
    return {
      'select': this.query.select,
      'from': [{
        'table': this.query.from
      }],
      'where': this.transformRule(this.query.where),
      'limit': this.query.limit,
      'offset': this.query.offset
    }
  }

  // This is inefficient, being called a lot
  isFieldSelected(field) {
    var fieldName = field.name;
    for (var i = 0; i < this.query.select.length; i++) {
      if (this.query.select[i].field == fieldName) {
        return true;
      }
    }
    return false;
  }

  toggleFieldSelection(event, field: Field) {
    var fieldName = field.name;
    var checked = event.checked;
    if (checked) {
      this.query.select.push({ "field": fieldName });
    } else {
      for (var i = 0; i < this.query.select.length; i++) {
        if (this.query.select[i].field == fieldName) {
          this.query.select.splice(i, 1);
          return;
        }
      }
    }
    this.view.queryChanged = true;
  }

  selectAllFields(b: boolean) {
    if (b) {
      var newSelect = [];
      for (var index in this.config.fields) {
        newSelect.push({ "field": this.config.fields[index].name });
      }
      this.query.select = newSelect;
    } else {
      this.query.select = [];
    }
    this.view.queryChanged = true;
  }

  public showJson(jsonObj: any, title: string): void {
    this.jsonDialogRef = this.dialog.open(JsonDialog, {
      width: '80%',
      height: '80%',
      data: { query: jsonObj, title: title }
    });
  }

  public showFields(): void {
    this.fieldsDialogRef = this.dialog.open(FieldsDialogComponent, {
      width: '80%',
      height: '80%',
      data: { fields: this.config.fields }
    });
  }

  public doQuery(query) {
    this.view.isQuerying = true;
    console.log("Original query\n" + JSON.stringify(query, null, 2))
    var transformedQuery = this.transformQuery()
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
