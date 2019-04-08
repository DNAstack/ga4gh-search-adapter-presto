import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

@Component({
  selector: 'json-dialog',
  templateUrl: './json-dialog.html'
})
export class JsonDialog {

  query: any;

  constructor(
    private dialogRef: MatDialogRef<FieldsDialog>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.query = data.query;
    this.jsonEditor.set(JSON.parse(JSON.stringify(this.query)));
  }

  ngOnInit() {
  }
}
