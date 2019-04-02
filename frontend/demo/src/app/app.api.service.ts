import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AppConfigService } from './app-config.service';
import { MatSnackBar } from '@angular/material';
import { catchError } from 'rxjs/operators';
import { ObservableInput } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ApiService {

  private readonly apiUrl;

  constructor(private httpClient: HttpClient,
              private app: AppConfigService,
              private snackBar: MatSnackBar) {
    this.apiUrl = app.config.apiUrl;
  }

  getFields() {
    return this.httpClient.get(`${this.apiUrl}/fields?table=demo_view`)
      .pipe(
        catchError((err) => this.errorSnack(err))
      );
  }

  doQuery(query) {
    return this.httpClient.post(`${this.apiUrl}/search`, query)
      .pipe(
        catchError((err) => this.errorSnack(err))
      );
  }

  private errorSnack(err): ObservableInput<{}> {
    this.snackBar.open(err.message, null, {
      panelClass: 'error-snack'
    });
    throw err;
  }
}
