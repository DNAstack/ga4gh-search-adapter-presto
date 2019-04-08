import { Component, Inject, OnInit } from '@angular/core';
import { Field } from '../../model/search/field';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

@Component({
  selector: 'fields-dialog',
  templateUrl: './fields-dialog.html'
})
export class FieldsDialog {

  fields: Field[];

  public view = {
    wrapFieldTableCells: true,
  }

  constructor(
    private dialogRef: MatDialogRef<FieldsDialog>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.fields = data.fields;
  }

  ngOnInit() {
  }
}
