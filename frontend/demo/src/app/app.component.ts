import { FormBuilder, FormControl } from '@angular/forms';
import { Component, OnInit } from '@angular/core';
import { ApiService } from './app.api.service';
import { Field } from './model/search/field';
import { Rule, RuleSet } from 'angular2-query-builder';
import { MatDialog, MatDialogRef, MatSnackBar } from '@angular/material';
import { JsonDialog } from './dialog/json/json-dialog';
import { FieldsDialogComponent } from './dialog/fields/fields-dialog.component';
import { AppConfigService } from './app-config.service';
import { SelectionModel } from '@angular/cdk/collections';
import { delay, repeat, switchMap, takeWhile } from 'rxjs/operators';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent implements OnInit {
  
  public sitename : string;
  public queryCtrl: FormControl;

  events: string[] = [];
  opened: boolean;

  runs = [];
  runStatus = {};

  public selection = new SelectionModel(true, []);

  public query = {
    select: [
        {
            table: "pgp_canada",
            name: "participant_id"
        },
        {
          table: "pgp_canada",
          name: "vcf_object"
        }
      ],
    from: 'pgp_canada',
    where: {
      condition: 'and',
      rules: [
        {
          "field": "pgp_canada.key",
          "operator": "=",
          "value": "Sex"
        },
        {
          "field": "pgp_canada.raw_value",
          "operator": "=",
          "value": "F"
        },
        {
          "field": "pgp_canada.chromosome",
          "operator": "=",
          "value": "chr1"
        },
        {
          "field": "pgp_canada.start_position",
          "operator": "=",
          "value": 5087263
        },
        {
          "field": "pgp_canada.reference_base",
          "operator": "=",
          "value": "A"
        },
        {
          "field": "pgp_canada.alternate_base",
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
    leftSidebarOpened: false,
    rightSidebarOpened : false,
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

  public workflows = [
    {
      name : "md5sum",
      inputs : [
        {
          id : "input_file",
          name : "File",
        }
      ],
      url : "http://localhost:8080/api/workflow/organization/108/project/125/workflowVersion/260"
    },
    {
      name : "DeepVariant",
      inputs : [
        {
          id : "input_files",
          name : "Files",
        }
      ],
      url : "http://localhost:8080/api/workflow/organization/108/project/125/workflowVersion/260"
    }
  ];

  public workflow = this.workflows[0];
  
  public results = null;

  private jsonDialogRef: MatDialogRef<JsonDialog>;
  private fieldsDialogRef: MatDialogRef<FieldsDialogComponent>;

  constructor(
    private app: AppConfigService,
    private formBuilder: FormBuilder,
    private apiService: ApiService,
    private dialog: MatDialog,
    private snackBar: MatSnackBar
  ) {
    this.sitename = app.config.sitename;
    this.view.showJSONs = app.config.developerMode;
    this.queryCtrl = this.formBuilder.control(this.query.where);
  }

  transformSelect(selectFields) {
    var newSelectFields = [];
    for (var i = 0; i < selectFields.length; i++) {
      newSelectFields.push({"field" : selectFields[i].table + "." + selectFields[i].name});
    }
    return newSelectFields;
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
    return JSON.parse(str);
  }

  paginationChanged(event) {
    this.query.limit = event.pageSize;
    this.query.offset = event.pageSize * event.pageIndex;
    this.view.queryChanged = true;
  }

  transformQuery() {
    return {
      'select': this.transformSelect(this.query.select),
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
    var tableName = field.table;
    var fieldName = field.name;
    for (var i = 0; i < this.query.select.length; i++) {
      if (this.query.select[i].table == tableName && this.query.select[i].name == fieldName) {
        return true;
      }
    }
    return false;
  }

  toggleFieldSelection(event, field: Field) {
    var fieldName = field.name;
    var checked = event.checked;
    if (checked) {
      this.query.select.push(field);
    } else {
      for (var i = 0; i < this.query.select.length; i++) {
        if (this.query.select[i].name == fieldName) {
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
        newSelect.push(this.config.fields[index]);
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

  public getDrsLabel(drs) {
    return JSON.parse(drs).name;
  }

  numWorkflowsSubmitted = 0;

  public doWorkflowExecution() {

    this.numWorkflowsSubmitted += this.selection.selected.length;

    for (var i = 0; i < this.selection.selected.length; i++) {

      var row = this.selection.selected[i].values;

      var params = {};

      for (var j = 0; j < this.workflow.inputs.length; j++) {
        var input: any = this.workflow.inputs[j];
        var field = input.mappedField;

        var fieldName = field.value.name;

        for (var k = 0; k < row.length; k++) {
          if (row[k].field.name == fieldName) {
            params[fieldName] = row[k].value;
          }
        }
      }

      var wes = {
        workflowUrl : this.workflow.url,
        parameters : params
      }

      this.apiService.getToken(i*5000).pipe(
        switchMap((token) => this.apiService.doWorkflowExecution(token, params))
      ).subscribe(
        ({run_id}) => this.startJobMonitor(run_id),
        ({error}) => this.snackError(error.msg || error.message)
      );
    }
  }

  public isSubmittingRuns() {
    return this.numWorkflowsSubmitted > this.runs.length;
  }

  startJobMonitor(runId) {
    this.runs.push(runId);
    this.runStatus[runId] = {
      state: 'INITIALIZING'
    };

    return this.apiService.getToken().pipe(
      switchMap((token) => this.apiService.monitorJob(token, runId)),
      delay(10000),
      repeat(100),
      takeWhile(({state}: any): any => {
        return state === 'INITIALIZING' || state === 'RUNNING';
      })
    ).subscribe(
      (res) => {
        this.runStatus[runId] = res;
      },
      ({error}) => {
        this.runStatus[runId] = 'CONNECTION ERROR';
        this.snackError(`Error getting run ${runId} status: ${error.msg || error.message}`)
      },
      () => {
        this.runStatus[runId] = {state: 'COMPLETE'};
      }
    )
  }

  public doQuery(query) {
    this.view.isQuerying = true;
    var transformedQuery = this.transformQuery()
    this.apiService.doQuery(transformedQuery).subscribe(
      (dto) => {
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

  validateInputMappings() {
    for (var j = 0; j < this.workflow.inputs.length; j++) {
      var input: any = this.workflow.inputs[j];
      if (!input.mappedField) {
        return false;
      } else if (!this.isFieldSelected(input.mappedField.value)) {
        return false;
      }
    }
    return true;
  }

  queryChanged($event) {
    this.view.queryChanged = true;
  }

  downloadDrs(drsStr) {
    var drs = JSON.parse(drsStr);
    var url = drs.urls[0].url;
    window.open(url);
  }

  normalizeArray<T>(array: Array<T>, indexKey: keyof T) {
    const normalizedObject: any = {}
    for (let i = 0; i < array.length; i++) {
      const key = array[i][indexKey]
      normalizedObject[key] = array[i]
    }
    return normalizedObject as { [key: string]: T }
  }

  /** Whether the number of selected elements matches the total number of rows. */
  isAllSelected() {
    const numSelected = this.selection.selected.length;
    const numRows = this.results.results.length;
    return numSelected == numRows;
  }

  /** Selects all rows if they are not all selected; otherwise clear selection. */
  masterToggle() {
    this.isAllSelected() ?
        this.selection.clear() :
        this.results.results.forEach(row => this.selection.select(row));
  }

  snack(message) {
    this.snackBar.open(message, "Dismiss", {
      panelClass: 'success-snack'
    });
  }

  snackError(message) {
    return this.snackBar.open(message, null, {
      panelClass: 'error-snack'
    });
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
