import { Component, Inject } from '@angular/core';
import { Field } from '../../model/search/field';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material';
import { JsonDialog } from '../../dialog/json/json-dialog';
import { AppConfigService } from '../../app-config.service';

@Component({
  templateUrl: './fields-dialog.component.html',
  styleUrls: ['./fields-dialog.component.scss'],
})
export class FieldsDialogComponent {

  fields: any;
  dataTable: Field[];
  displayedColumns: string[] = ['id', 'name', 'type'];
  public view;

  private jsonDialogRef: MatDialogRef<JsonDialog>;

  constructor(
    private app: AppConfigService,
    private dialog: MatDialog,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    //this.view.showJSONs = app.config.developerMode;
    console.log(app.config);

    this.view = {showJSONs: app.config.developerMode };
    this.fields = this.data.fields;
    this.dataTable = Object.keys(this.data.fields)
      .map(fieldKey => this.data.fields[fieldKey]);
  }

  public showJson(jsonObj: any, title : string): void {
    this.jsonDialogRef = this.dialog.open(JsonDialog, {
      width: '80%',
      height: '80%',
      data: { query: jsonObj, title: title }
    });
  }
}
