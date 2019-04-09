import { Component, Inject } from '@angular/core';
import { Field } from '../../model/search/field';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

@Component({
  templateUrl: './fields-dialog.component.html',
  styleUrls: ['./fields-dialog.component.scss'],
})
export class FieldsDialogComponent {

  dataTable: Field[];
  displayedColumns: string[] = ['id', 'name', 'type', 'value', 'operators', 'specification'];

  constructor(
    private dialogRef: MatDialogRef<FieldsDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.dataTable = Object.keys(this.data.fields)
      .map(fieldKey => this.data.fields[fieldKey]);
  }
}
