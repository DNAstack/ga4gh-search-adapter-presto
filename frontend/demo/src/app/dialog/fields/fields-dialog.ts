import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { FormGroup } from '@angular/forms';

@Component({
  selector: 'fields-dialog',
  templateUrl: './fields-dialog.html'
})
export class FieldsDialog {

  form: FormGroup;
  foo: any;

  constructor(
    private dialogRef: MatDialogRef<FieldsDialog>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.foo = data.foo;
  }

  ngOnInit() {
  }
}
